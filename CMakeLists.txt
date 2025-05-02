cmake_minimum_required(VERSION 3.14)
project(kv_store)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Output binaries to ./build/bin/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Add the include directory
include_directories(${CMAKE_SOURCE_DIR}/include)

# Add your main executable
add_executable(toy_kv_store
    src/main.cpp
    src/file_handle.cpp
    src/log_writer.cpp
)