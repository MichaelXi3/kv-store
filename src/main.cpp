#include <iostream>
#include "kv/log_writer.hpp"
#include "kv/memtable.hpp"
#include "kv/file_handle.hpp"
#include "kv/kv_store.hpp"

int main() {
    // Test FileHandle
    std::cout << "\n--- Testing FileHandle ---" << std::endl;
    kv::FileHandle fd("filehandle_test.log");
    std::string data = "put(Michael, 1)\n";

    for (int i = 0; i < 5; ++i) {
        fd.write(data);
    }
    fd.printContent();

    // Test LogWriter
    std::cout << "\n--- Testing LogWriter ---" << std::endl;
    kv::LogWriter logWriter("logwriter_test.log");
    logWriter.appendRecord(data);

    // Test MemTable
    std::cout << "\n--- Testing MemTable ---" << std::endl;
    kv::MemTable memTable;
    memTable.put("Mike", "1");
    memTable.put("Mike", "2");
    std::cout << "Size of memTable: " << memTable.size() << "\n" << std::endl;

    memTable.put("Jack", "1");
    memTable.put("Jack", "3");
    std::cout << "Size of memTable: " << memTable.size() << "\n" << std::endl;

    auto v1 = memTable.get("Mike");
    auto v2 = memTable.get("Jack");
    std::cout << "Key: Mike, Value: " << *v1 << "\n" << std::endl;
    std::cout << "Key: Jack, Value: " << *v2 << "\n" << std::endl;

    // Test KVStore
    std::cout << "\n--- Testing KVStore ---" << std::endl;
    kv::KVStore store("testdb");
    store.put("Alice", "100");
    store.put("Bob", "200");
    store.put("Alice", "300");

    auto val1 = store.get("Alice");
    auto val2 = store.get("Bob");
    auto val3 = store.get("Charlie");

    std::cout << "Key: Alice, Value: " << (val1 ? *val1 : "<not found>") << std::endl;
    std::cout << "Key: Bob, Value: " << (val2 ? *val2 : "<not found>") << std::endl;
    std::cout << "Key: Charlie, Value: " << (val3 ? *val3 : "<not found>") << std::endl;

    // Test KVStore WAL replay
    std::cout << "\n--- Testing KVStore WAL Replay ---" << std::endl;
    kv::KVStore store2("testdb"); // This should replay the WAL
    auto val1_replay = store2.get("Alice");
    auto val2_replay = store2.get("Bob");
    std::cout << "After replay - Key: Alice, Value: " << (val1_replay ? *val1_replay : "<not found>") << std::endl;
    std::cout << "After replay - Key: Bob, Value: " << (val2_replay ? *val2_replay : "<not found>") << std::endl;

    return 0;
}