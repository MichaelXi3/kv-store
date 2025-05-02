/**
 * @file file_handle.hpp
 * @brief RAII wrapper for file output streams used in the KV store.
 *
 * FileHandle manages a std::ofstream instance, ensuring that the file is
 * automatically opened in append mode during construction and closed during
 * destruction. This simplifies safe and consistent file I/O for components
 * like LogWriter or SSTableWriter, which depend on reliable file access.
 *
 * Typical usage:
 *   kv::FileHandle fd("wal.log");
 *   fd.get() << "put(key, value)\n";
 *
 * Used by:
 *   - LogWriter: to write WAL records
 *   - SSTableWriter: to flush MemTable to disk
 */
#pragma once
#include <fstream>

namespace kv {

class FileHandle {
public:
    explicit FileHandle(const std::string& filePath); // Constructor
    ~FileHandle();                                    // Destructor

    std::ofstream& get();                             // Returns the ofstream reference
    void write(const std::string& data);              // Writes data to the file and flushes it
    void printContent() const;                        // Prints the content of the file

private:
    std::string _filePath;                            // Path to the file
    std::ofstream _file;                              // ofstream instance
};

} // namespace kv