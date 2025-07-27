#include "kv/sstable_writer.hpp"
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace kv {

SSTableWriter::SSTableWriter(const std::string& data_dir)
    : _data_dir(data_dir) 
{
    std::filesystem::create_directories(data_dir);
}

// turn file_number to 8 digits file name
std::string SSTableWriter::makeFileName(uint64_t file_number) const {
    std::ostringstream oss; // use oss to build string
    oss << std::setw(8) << std::setfill('0') << file_number << ".sst";
    return oss.str();
}

bool SSTableWriter::writeSSTable(const std::map<std::string, std::string>& sorted_data, uint64_t file_number) {
    std::string file_name = _data_dir + "/" + makeFileName(file_number);

    // std::ios::binary: Opens the file for binary (raw byte) input/output, preventing character translations.
    // std::ios::trunc: If the file exists, its contents are deleted (truncated) before writing. If it doesn't exist, create a new empty file.
    std::ofstream out(file_name, std::ios::trunc | std::ios::binary);

    if (!out.is_open()) {
        std::cerr << "[SSTableWriter] Cannot open file: " << file_name << "\n";
        return false;
    }

    // write format: [key_len][key_data][value_len][value_data]
    for (const auto& [key, value] : sorted_data) {
        uint32_t key_len = static_cast<uint32_t>(key.size());
        uint32_t value_len = static_cast<uint32_t>(value.size());

        out.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
        out.write(key.data(), key_len);

        out.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
        out.write(value.data(), value_len);
    }

    out.flush();
    out.close();
    return true;
}

}