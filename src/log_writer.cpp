#include "kv/log_writer.hpp"
#include "kv/file_handle.hpp"
#include <fstream>
#include <iostream>

namespace kv {

// Constructor: Opens the file in append mode
LogWriter::LogWriter(const std::string& filePath) 
    : _fileHandle {filePath} {
};

LogWriter::~LogWriter() = default;

void LogWriter::appendRecord(const std::string &record) {
    // Acquire lock and release on scope exit
    std::lock_guard<std::mutex> lock(_mutex);
    _fileHandle.write(record);
}

} // namespace kv