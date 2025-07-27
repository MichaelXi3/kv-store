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

void testWALReplay() {
    std::cout << "\n--- Testing KVStore ---" << std::endl;
    std::string kvstore_path = TEST_DIR + "/test_wal_replay";
    
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
    std::string test_sstable_dir = TEST_DIR + "/test_sstable_flusher";
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

void testSSTableReader() {
    std::cout << "\n--- Testing SSTable Reader (Both Read Cases) ---" << std::endl;
    
    std::string test_db_path = TEST_DIR + "/test_sstable_reader";
    
    // Clean up any existing test data
    if (std::filesystem::exists(test_db_path)) {
        std::filesystem::remove_all(test_db_path);
    }
    
    // Phase 1: Create some SSTable files with known data (on-disk data)
    std::cout << "\n1. Creating SSTable files with sample data..." << std::endl;
    std::filesystem::create_directories(test_db_path);
    
    kv::SSTableWriter writer(test_db_path);
    
    // Create first SSTable with some data
    kv::MemTable temp_table1;
    temp_table1.put("disk_key1", "disk_value1");
    temp_table1.put("disk_key2", "disk_value2");
    temp_table1.put("zebra", "last_alphabetical");
    
    // Convert MemTable to std::map for SSTableWriter
    std::map<std::string, std::string> sorted_data1;
    for (const auto& [key, value] : temp_table1.data()) {
        sorted_data1[key] = value;
    }
    writer.writeSSTable(sorted_data1, 1);
    
    // Create second SSTable with different data
    kv::MemTable temp_table2;
    temp_table2.put("disk_key3", "disk_value3");
    temp_table2.put("apple", "first_alphabetical");
    temp_table2.put("disk_key1", "newer_disk_value1"); // This should override the first one
    
    // Convert MemTable to std::map for SSTableWriter
    std::map<std::string, std::string> sorted_data2;
    for (const auto& [key, value] : temp_table2.data()) {
        sorted_data2[key] = value;
    }
    writer.writeSSTable(sorted_data2, 2);
    
    std::cout << "   Created 2 SSTable files with sample data" << std::endl;
    
    // Phase 2: Create KVStore and add some in-memory data
    std::cout << "\n2. Creating KVStore and adding in-memory data..." << std::endl;
    
    kv::KVStore store(test_db_path);
    
    // Add some data that will stay in memory
    store.put("memory_key1", "memory_value1");
    store.put("memory_key2", "memory_value2");
    store.put("disk_key1", "latest_memory_value1"); // This should override disk version
    
    std::cout << "   Added 3 keys to in-memory hash table" << std::endl;
    
    // Phase 3: Test different read scenarios
    std::cout << "\n3. Testing read scenarios:" << std::endl;
    
    // Test Case 1: Read from in-memory hash table (should hit memory first)
    std::cout << "\n   Test Case 1 - Reading keys that exist in memory:" << std::endl;
    auto result1 = store.get("memory_key1");
    auto result2 = store.get("memory_key2");
    auto result3 = store.get("disk_key1"); // This exists in both, should get memory version
    
    std::cout << "   memory_key1: " << (result1 ? *result1 : "<not found>") << std::endl;
    std::cout << "   memory_key2: " << (result2 ? *result2 : "<not found>") << std::endl;
    std::cout << "   disk_key1 (should be memory version): " << (result3 ? *result3 : "<not found>") << std::endl;
    
    // Test Case 2: Read from on-disk SSTables (should fall through to disk)
    std::cout << "\n   Test Case 2 - Reading keys that exist only on disk:" << std::endl;
    auto result4 = store.get("disk_key2");
    auto result5 = store.get("disk_key3");
    auto result6 = store.get("apple");
    auto result7 = store.get("zebra");
    
    std::cout << "   disk_key2 (from disk): " << (result4 ? *result4 : "<not found>") << std::endl;
    std::cout << "   disk_key3 (from disk): " << (result5 ? *result5 : "<not found>") << std::endl;
    std::cout << "   apple (from disk): " << (result6 ? *result6 : "<not found>") << std::endl;
    std::cout << "   zebra (from disk): " << (result7 ? *result7 : "<not found>") << std::endl;
    
    // Test Case 3: Read non-existent keys (should not be found anywhere)
    std::cout << "\n   Test Case 3 - Reading keys that don't exist:" << std::endl;
    auto result8 = store.get("nonexistent_key");
    auto result9 = store.get("missing_key");
    
    std::cout << "   nonexistent_key: " << (result8 ? *result8 : "<not found>") << std::endl;
    std::cout << "   missing_key: " << (result9 ? *result9 : "<not found>") << std::endl;
    
    // Phase 4: Verify expected results
    std::cout << "\n4. Verifying test results:" << std::endl;
    
    bool all_tests_passed = true;
    
    // Verify memory reads
    if (!result1 || *result1 != "memory_value1") {
        std::cout << "   ❌ FAIL: memory_key1 should return 'memory_value1'" << std::endl;
        all_tests_passed = false;
    } else {
        std::cout << "   ✅ PASS: memory_key1 read from memory" << std::endl;
    }
    
    // Verify memory override of disk data
    if (!result3 || *result3 != "latest_memory_value1") {
        std::cout << "   ❌ FAIL: disk_key1 should return memory version 'latest_memory_value1'" << std::endl;
        all_tests_passed = false;
    } else {
        std::cout << "   ✅ PASS: disk_key1 correctly returns memory version (overrides disk)" << std::endl;
    }
    
    // Verify disk reads
    if (!result4 || *result4 != "disk_value2") {
        std::cout << "   ❌ FAIL: disk_key2 should return 'disk_value2' from disk" << std::endl;
        all_tests_passed = false;
    } else {
        std::cout << "   ✅ PASS: disk_key2 read from SSTable" << std::endl;
    }
    
    if (!result5 || *result5 != "disk_value3") {
        std::cout << "   ❌ FAIL: disk_key3 should return 'disk_value3' from disk" << std::endl;
        all_tests_passed = false;
    } else {
        std::cout << "   ✅ PASS: disk_key3 read from SSTable" << std::endl;
    }
    
    // Verify non-existent keys
    if (result8 || result9) {
        std::cout << "   ❌ FAIL: Non-existent keys should return nullopt" << std::endl;
        all_tests_passed = false;
    } else {
        std::cout << "   ✅ PASS: Non-existent keys correctly return not found" << std::endl;
    }
    
    if (all_tests_passed) {
        std::cout << "\nSSTable Reader test completed successfully!" << std::endl;
    } else {
        std::cout << "\nSome SSTable Reader tests failed!" << std::endl;
    }
}

int main() {
    setupTestDir();
    
    try {
        testFileHandle();
        testLogWriter();
        testMemTable();
        testWALReplay();
        testFlusher();
        testSSTableReader();
        
        std::cout << "\n=== All tests completed successfully! ===" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
    }
    
    cleanupTestDir();
    return 0;
}