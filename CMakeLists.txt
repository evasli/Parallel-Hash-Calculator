cmake_minimum_required(VERSION 3.15)
project(ParallelHashProcessor)

set(CMAKE_CXX_STANDARD 17)

# 1. Standard Packages (These usually work fine)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(simdjson CONFIG REQUIRED)

# 2. The "Hybrid" find for SimpleAmqpClient
# Try finding it the official way first (for GitHub users)
find_package(simpleamqpclient CONFIG QUIET)

if(simpleamqpclient_FOUND)
    set(AMQP_LIB simpleamqpclient::simpleamqpclient)
    message(STATUS "Found SimpleAmqpClient via Manifest/Config")
else()
    # FALLBACK: Use your local hardcoded path (for your machine)
    set(AMQP_LIB "C:/Users/HP/Documents/Clibraries/vcpkg/installed/x64-windows/lib/SimpleAmqpClient.7.lib")
    set(RABBIT_LIB "C:/Users/HP/Documents/Clibraries/vcpkg/installed/x64-windows/lib/rabbitmq.4.lib")
    include_directories("C:/Users/HP/Documents/Clibraries/vcpkg/installed/x64-windows/include")
    message(STATUS "Using local hardcoded fallback for SimpleAmqpClient")
endif()

add_executable(cpp_app ParallelHashCalculator.cpp)

target_link_libraries(cpp_app PRIVATE
    nlohmann_json::nlohmann_json
    TBB::tbb
    OpenSSL::SSL
    OpenSSL::Crypto
    simdjson::simdjson
    ${AMQP_LIB}
    ${RABBIT_LIB}
)

# Copy data.json to the build folder automatically
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/data.json"
    "${CMAKE_CURRENT_BINARY_DIR}/data.json"
    COPYONLY
)