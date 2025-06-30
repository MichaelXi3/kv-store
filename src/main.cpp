#include <iostream>
#include <filesystem>
#include <chrono>
#include "kv/log_writer.hpp"
#include "kv/memtable.hpp"
#include "kv/file_handle.hpp"
#include "kv/kv_store.hpp"
#include "kv/sstable_writer.hpp"
#include "kv/flusher.hpp"

// Test directory management
const std::string TEST_DIR = "test_temp_dir";

void setupTestDir() {
    std::filesystem::create_directories(TEST_DIR);
    std::cout << "Created test directory: " << TEST_DIR << std::endl;
}

void cleanupTestDir() {
    // if (std::filesystem::exists(TEST_DIR)) {
    //     std::filesystem::remove_all(TEST_DIR);
    //     std::cout << "Cleaned up test directory: " << TEST_DIR << std::endl;
    // }
}

void testFileHandle() {
    std::cout << "\n--- Testing FileHandle ---" << std::endl;
    kv::FileHandle fd(TEST_DIR + "/test_filehandle.log");
    std::string data = "put(Michael, 1)\n";

    for (int i = 0; i < 5; ++i) {
        fd.write(data);
    }
    fd.printContent();
    std::cout << "FileHandle test completed." << std::endl;
}

void testLogWriter() {
    std::cout << "\n--- Testing LogWriter ---" << std::endl;
    kv::LogWriter logWriter(TEST_DIR + "/test_logwriter.log");
    std::string data = "put(Michael, 1)\n";
    logWriter.appendRecord(data);
    std::cout << "LogWriter test completed." << std::endl;
}

void testMemTable() {
    std::cout << "\n--- Testing MemTable ---" << std::endl;
    kv::MemTable memTable;
    memTable.put("Mike", "1");
    memTable.put("Mike", "2");
    std::cout << "Size of memTable: " << memTable.size() << std::endl;

    memTable.put("Jack", "1");
    memTable.put("Jack", "3");
    std::cout << "Size of memTable: " << memTable.size() << std::endl;

    auto v1 = memTable.get("Mike");
    auto v2 = memTable.get("Jack");
    std::cout << "Key: Mike, Value: " << *v1 << std::endl;
    std::cout << "Key: Jack, Value: " << *v2 << std::endl;
    std::cout << "MemTable test completed." << std::endl;
}

void testKVStore() {
    std::cout << "\n--- Testing KVStore ---" << std::endl;
    std::string kvstore_path = TEST_DIR + "/test_kvstore_db";
    
    {
        kv::KVStore store(kvstore_path);
        store.put("Alice", "100");
        store.put("Bob", "200");
        store.put("Alice", "300");

        auto val1 = store.get("Alice");
        auto val2 = store.get("Bob");
        auto val3 = store.get("Charlie");

        std::cout << "Key: Alice, Value: " << (val1 ? *val1 : "<not found>") << std::endl;
        std::cout << "Key: Bob, Value: " << (val2 ? *val2 : "<not found>") << std::endl;
        std::cout << "Key: Charlie, Value: " << (val3 ? *val3 : "<not found>") << std::endl;
    }

    // Test WAL replay = = SIMULATED PROCESS RESTART
    std::cout << "\n--- Testing KVStore WAL Replay ---" << std::endl;
    {
        kv::KVStore store2(kvstore_path); // This should replay the WAL
        auto val1_replay = store2.get("Alice");
        auto val2_replay = store2.get("Bob");
        std::cout << "After replay - Key: Alice, Value: " << (val1_replay ? *val1_replay : "<not found>") << std::endl;
        std::cout << "After replay - Key: Bob, Value: " << (val2_replay ? *val2_replay : "<not found>") << std::endl;
    }
    std::cout << "KVStore test completed." << std::endl;
}

void testFlusher() {
    std::cout << "\n--- Testing Flusher ---" << std::endl;
    // 1) The active memtable and mutexes
    auto mem = std::make_shared<kv::MemTable>();
    std::mutex active_mtx, immu_mtx;
    
    // 2) Init writer for test SSTable files
    std::string test_sstable_dir = TEST_DIR + "/test_sstables";
    kv::SSTableWriter writer(test_sstable_dir);
    kv::Flusher flusher(mem, active_mtx, immu_mtx, writer, 100);
    flusher.start();
    
    // 3) Simulate writes
    for (int i = 0; i < 600; i++) {
        {
            std::lock_guard lk(active_mtx);
            mem->put("key" + std::to_string(i), "value" + std::to_string(i));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    // 4) Stop the flusher
    flusher.stop();
    std::cout << "Flusher test completed." << std::endl;
    std::cout << "SSTable files written to: " << test_sstable_dir << std::endl;
}

int main() {
    setupTestDir();
    
    try {
        // Comment/uncomment individual tests as needed
        testFileHandle();
        testLogWriter();
        testMemTable();
        testKVStore();
        testFlusher();
        
        std::cout << "\n=== All tests completed successfully! ===" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
    }
    
    cleanupTestDir();
    return 0;
}