#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <iomanip>
#include <sstream>
#include <atomic>
#include <thread>
#include <chrono>
#include <mutex>
#include <ctime>

// Libraries for hashing, RabbitMQ, JSON parsing, and TBB
#include <openssl/sha.h>
#include <tbb/tbb.h>
#include <nlohmann/json.hpp>
#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include "simdjson.h"

using json = nlohmann::json;
using namespace AmqpClient;

struct DataItem {
    int id;
    std::string text;
    std::string sha512;
    std::string sha256;
};

std::vector<DataItem> dataset;
std::atomic<int> received_count(0);
const std::string results_queue = "results_queue";
const std::string tasks_queue = "tasks_queue";
const std::string shutdown_queue = "cluster_control_queue";

std::string calculate_sha512(const std::string& input) {
    unsigned char hash[SHA512_DIGEST_LENGTH];
    SHA512_CTX sha512;
    SHA512_Init(&sha512);
    SHA512_Update(&sha512, input.c_str(), input.size());
    SHA512_Final(hash, &sha512);

    std::stringstream ss;
    for (int i = 0; i < SHA512_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return ss.str();
}

int main() {
	int initial_strings_count = 1200; // Number of initial strings in JSON file
    dataset.reserve(initial_strings_count);
    std::mutex dataset_mutex;
    int scale_factor = 10000;
	auto max_tokens = 10; // Max number of active tasks in the pipeline

    size_t num_threads = 8;
	tbb::global_control control(tbb::global_control::max_allowed_parallelism, num_threads); // Limit TBB to specific number of threads

	// Prepare to read large JSON file in streaming mode
    simdjson::ondemand::parser parser; 
    auto load_result = simdjson::padded_string::load("data.json");
    if (load_result.error()) { return 1; }
    simdjson::padded_string& json_data = load_result.value();

    auto document_stream_result = parser.iterate_many(
        reinterpret_cast<const uint8_t*>(json_data.data()),
        json_data.size(),
		1 * 1024 * 1024 // Batch size of 1MB
    );

    if (document_stream_result.error()) {
        std::cerr << "Stream error: " << document_stream_result.error() << std::endl;
        return 1;
    }

    // Get the actual document_stream object
    auto& document_stream = document_stream_result.value();
    auto it = document_stream.begin();
    int id_counter = 0;
	
    std::cout << "Starting parallel part: reading JSON file in stream, calculating SHA512 and sending to RabbitMQ "<< std::endl;

	// Connections for RabbitMQ publishing and subscribing
    Channel::ptr_t pub_channel = Channel::Create("localhost");
    pub_channel->DeclareQueue(tasks_queue, false, false, false, false);
    Channel::ptr_t sub_channel = Channel::Create("localhost");
    sub_channel->DeclareQueue(results_queue, false, false, false, false);

    auto start_time = tbb::tick_count::now();
    std::time_t now = std::time(nullptr);
    std::cout << "Time now: " << std::put_time(std::localtime(&now), "%H:%M:%S") << std::endl;


	// Consumer thread that works in the background to receive SHA256 results from Python workers
    std::thread consumer_thread([&]() {
        try {
            std::string consumer_tag = sub_channel->BasicConsume(results_queue);

            while (received_count < initial_strings_count) {
				Envelope::ptr_t envelope = sub_channel->BasicConsumeMessage(consumer_tag); // Blocking wait until message arrives
                std::string body = envelope->Message()->Body();

                // Parsing JSON from python: {"id": , "sha256": }
                auto resp = json::parse(body);
                int id = resp["id"];
                dataset[id].sha256 = resp.at("sha256").get<std::string>();
                received_count++;

                if (received_count % 500 == 0) {
                    std::cout << "[C++] Received SHA256 messages: " << received_count << "/" << dataset.size() << "\r" << std::flush;
                }
            }
        }
        catch (const std::exception& e) {
            std::cerr << "Consumer thread error: " << e.what() << std::endl;
        }
        });

    auto pipeline_start = tbb::tick_count::now();

    // Pipeline: parsing -> hashing -> publishing
    tbb::parallel_pipeline(max_tokens,
        // Stage 1: stream reader (serial)
        tbb::make_filter<void, DataItem*>(tbb::filter_mode::serial_in_order,
            [&](tbb::flow_control& fc) -> DataItem* {
                if (it != document_stream.end()) {
                    auto doc_res = *it;
                    if (doc_res.error()) {
                        fc.stop();
						std::cout << "Error reading JSON: " << doc_res.error() << std::endl;
                        return nullptr;
                    }
                    auto doc = doc_res.value();
                    auto string_res = doc.get_string(); // Get the string value from the line
                    if (string_res.error()) {
                        ++it;    // If line wasn't a valid string, move to next
                        return nullptr;
                    }

                    DataItem* item = new DataItem();
                    item->id = id_counter++;

                    item->text = std::string(string_res.value());
                    {
                        std::lock_guard<std::mutex> lock(dataset_mutex);;
                        dataset.push_back({ item->id, "", "", "" });
                    }

                    ++it; // Move to the next JSON object in the stream
                    return item;
                }
                fc.stop();
                return nullptr;
            }
        )&
        // Stage 2: SHA512 (parallel)
        tbb::make_filter<DataItem*, DataItem*>(tbb::filter_mode::parallel,
            [&](DataItem* item) -> DataItem* {
                std::string_view original = item->text;

				// Repeat the string to increase its size
                std::string new_longer_text;
                new_longer_text.reserve(original.size()* scale_factor);

				for (int i = 0; i < scale_factor; i++) { 
                    new_longer_text.append(original);
                }
                item->sha512 = calculate_sha512(new_longer_text);
                dataset[item->id].sha512 = std::move(item->sha512);
                return item;
            }
        ) &
        // Stage 3: RabbitMQ publish and cleanup (serial)
        tbb::make_filter<DataItem*, void>(tbb::filter_mode::serial_in_order,
            [&](DataItem* item) {
                try {
					// Creating JSON object
                    nlohmann::json j;
                    j["id"] = item->id;
                    j["string"] = std::move(item->text);

                    std::string serialized_json = j.dump(); // Converting JSON object to string

                    auto message = AmqpClient::BasicMessage::Create(std::move(serialized_json));
                    pub_channel->BasicPublish("", "tasks_queue", message);
                }
                catch (const std::exception& e) {
                    std::cerr << "Publish error: " << e.what() << std::endl;
                }
                delete item;
            }
        )
    );

    std::cout << "[C++] All tasks sent. Waiting for final responses..." << std::endl;

    // Wait for consumer to finish
    if (consumer_thread.joinable()) consumer_thread.join();

    // Notify Python to stop
    pub_channel->BasicPublish("", shutdown_queue, BasicMessage::Create("shutdown"));

    auto end_time = tbb::tick_count::now();
    std::cout << "\nParallel part done in: " << (end_time - start_time).seconds() << " s" << std::endl;
    std::time_t now_time_end = std::time(nullptr);
    std::cout << "Time now: " << std::put_time(std::localtime(&now_time_end), "%H:%M:%S") << std::endl;

    // Print results to file
    std::ofstream out("results.txt");
    if (out.is_open()) {
        for (const auto& item : dataset) {
            out << "ID: " << item.id << "\n";
            out << " | SHA512: " << item.sha512 << "\n";
            out << " | SHA256: " << item.sha256 << "\n\n";

        }
        out.close();
        std::cout << "Results writen to results.txt" << std::endl;
    }

    return 0;
}