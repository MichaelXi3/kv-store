
#include <iostream>
#include "kv/file_handle.hpp"
#include "kv/log_writer.hpp"

int main() {
    kv::FileHandle fd("filehandle_test.log");
    std::string data = "put(Michael, 1)\n";

    for (int i = 0; i < 5; ++i) {
        fd.write(data);
    }
    
    fd.printContent();

    kv::LogWriter logWriter("logwriter_test.log");
    logWriter.appendRecord(data);
    return 0;
}