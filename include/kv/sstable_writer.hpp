/**
 * @file sstable_writer.hpp
 * @brief Writes SSTable files to disk.
 *
 * SSTableWriter is responsible for writing SSTable files to disk.
 * It takes a map of key-value pairs and writes them to a file.
 * The file is named according to the file number and the current timestamp.
 * The file is written to the data directory.
 */

#pragma once
#include <string>
#include <map>
#include <cstdint>

class SSTableWriter {
public:
    explicit SSTableWriter(const std::string& data_dir);

    bool writeSSTable(const std::map<std::string, std::string>& data, uint64_t file_number);

private:
    std::string _data_dir;
    std::string makeFileName(uint64_t file_number) const;
}