import hashlib
import json
import time
import pika
import ray

INPUT_QUEUE = "tasks_queue"
OUTPUT_QUEUE = "results_queue"
CLUSTER_CONTROL_QUEUE = "cluster_control_queue"
SCALE_FACTOR = 10000

class HashingWorker:
    def __init__(self, parameters: pika.ConnectionParameters) -> None:
        self.connection = pika.BlockingConnection(parameters)
        self.results_channel = self.connection.channel()
        self.results_channel.queue_declare(queue=OUTPUT_QUEUE)

        self.data_channel = self.connection.channel()
        self.data_channel.queue_declare(queue=INPUT_QUEUE)
        
        self.data_channel.basic_qos(prefetch_count=10) # Limit unacknowledged messages to 10 for one actor

        self.start_time = None
        self.total_messages_processed = 0
        self.pure_hashing_time = 0.0  # Accumulate only hash time

    def start_consuming(self) -> None:
        self.data_channel.basic_consume(
            queue=INPUT_QUEUE, on_message_callback=self.process_string
        )
        self.data_channel.start_consuming()

    def process_string(self, ch, method, properties, body) -> None:
        if self.start_time is None:
            self.start_time = time.time()
            print(f"Started now: {time.strftime('%H:%M:%S', time.gmtime(self.start_time))}")

        try:
            data = json.loads(body)
            
            msg_id = data.get("id")
            text_to_hash = data.get("string")

            if msg_id is None or text_to_hash is None:
                print(f"Invalid message structure!")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
                        
            content_bytes = (text_to_hash.encode() * SCALE_FACTOR) # Repeat to increase size

            hash_start = time.perf_counter()
            hashed_message = hashlib.sha256(content_bytes).hexdigest()
            hash_duration = time.perf_counter() - hash_start
            self.pure_hashing_time += hash_duration

            self.send_result(msg_id, hashed_message)
        
        except Exception as e:
            print(f"Error: {e}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.total_messages_processed += 1
        if self.total_messages_processed % 100 == 0:
            total_time = time.time() - self.start_time
            print(f"Actor processed {self.total_messages_processed} msgs. All time elapsed: {total_time:.2f}s")
            print(f"Pure hashing time: {self.pure_hashing_time:.4f}s")

    # Format to JSON and send to output queue
    def send_result(self, msg_id: int, result: str) -> None:
        response_message = {"id": msg_id, "sha256": result}
        serialized = json.dumps(response_message)
        self.results_channel.basic_publish(
            exchange="", 
            routing_key=OUTPUT_QUEUE, 
            body=serialized
        )

def get_message_count(ch, queue_name: str) -> int:
    queue_state = ch.queue_declare(queue=queue_name, passive=True)
    return queue_state.method.message_count

def cpp_message(ch, method, properties, body) -> None:
    message = body.decode()
    print(f"Received control message: {message}")
    if message == "shutdown":
        while (msg_num := get_message_count(ch, INPUT_QUEUE)) > 0:
            print(f"Waiting for {msg_num} messages to be processed before shutdown...")
            time.sleep(1)
        print(f"Shutdown complete: {time.strftime('%H:%M:%S', time.gmtime(time.time()))}")
        ray.shutdown()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()
        ch.close()


def start(connection_parameters: pika.ConnectionParameters, num_workers=1) -> None:
    connection: pika.BlockingConnection = pika.BlockingConnection(connection_parameters)
    print("Initializing Ray...")
    ray.init()
    print("Ray initialized.")
    ParallelHashingWorker: ActorClass[HashingWorker] = ray.remote(HashingWorker)
    workers: list[ActorProxy[HashingWorker]] = [
        ParallelHashingWorker.remote(connection_parameters) for _ in range(num_workers)
    ]
    print(f"Started {num_workers} workers.")
    for worker in workers:
        worker.start_consuming.remote()

    channel = connection.channel()
    if not channel.is_open:
        print(f"Failed to open channel!")
        return
    channel.queue_declare(queue=CLUSTER_CONTROL_QUEUE)
    channel.basic_consume(queue=CLUSTER_CONTROL_QUEUE, on_message_callback=cpp_message)
    channel.start_consuming()
    connection.close()


if __name__ == "__main__":
    connection_parameters = pika.ConnectionParameters(host="192.168.8.124", port=5672) # Host can be changed to localhost if running locally
    start(connection_parameters, num_workers=6)
