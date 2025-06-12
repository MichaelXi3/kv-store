cmake_minimum_required(VERSION 3.14)
project(kv_store 
    VERSION 0.1.0
    DESCRIPTION "A simple key-value store implementation"
    LANGUAGES CXX
)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Set output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Create a target for the include directory
add_library(kv_store_headers INTERFACE)
target_include_directories(kv_store_headers
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Collect all source files
set(SOURCES
    src/main.cpp
    src/file_handle.cpp
    src/log_writer.cpp
    src/memtable.cpp
    src/kv_store.cpp
    src/sstable_writer.cpp
    src/flusher.cpp
)

# Create the executable
add_executable(toy_kv_store ${SOURCES})

# Link against the headers
target_link_libraries(toy_kv_store PRIVATE kv_store_headers)

# Enable testing
enable_testing()

# Installation rules
install(TARGETS toy_kv_store
    RUNTIME DESTINATION bin
)

install(TARGETS kv_store_headers
    EXPORT kv_store-targets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

install(DIRECTORY include/
    DESTINATION include
    FILES_MATCHING PATTERN "*.hpp"
)

# Add include directory to the target directly
target_include_directories(toy_kv_store PRIVATE ${CMAKE_SOURCE_DIR}/include)