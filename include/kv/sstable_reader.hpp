/* 
 * SSTable Reader need to be able to read across sstables to find value.
 * base on the key. It should take KEY input, and has knowledge about
 * all SSTables, and read it from the most recent one to the oldest one,
 * which ensures the latest and correct data returned. Note that the read
 * should first hit the in-memory table, then fall to the SSTables.
 * 
 * - It needs to know where does the SSTables reside.
 * - It needs to know what SSTables look like.
 * - It needs to be able to quickly locate the table that contains the key.
 * 
 * Future
 * - It needs to be able to detect corruption.
 */
#pragma once
#include <string>

namespace kv {

class SSTableReader {
public:
    explicit SSTableReader(const std::string& data_dir);

    // interface api for read value based on key
    std::optional<std::string> get_from_sstable(const std::string& key);

private:
    std::string _data_dir;

    // Discover and cache SSTable file list (newest first)
    std::vector<std::string> getSSTableFiles() const;

    // Read from a specific SSTable
    std::optional<std::string> readFromSSTable(std::string& sstable_name, std::string& key);
};

} // namespace kv

