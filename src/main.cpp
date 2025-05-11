
#include <iostream>
#include "kv/log_writer.hpp"
#include "kv/memtable.hpp"
#include "kv/file_handle.hpp"

int main() {
    // Test FileHandle
    kv::FileHandle fd("filehandle_test.log");
    std::string data = "put(Michael, 1)\n";

    for (int i = 0; i < 5; ++i) {
        fd.write(data);
    }
    fd.printContent();

    // Test LogWriter
    kv::LogWriter logWriter("logwriter_test.log");
    logWriter.appendRecord(data);

    // MemTable
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

    return 0;
}