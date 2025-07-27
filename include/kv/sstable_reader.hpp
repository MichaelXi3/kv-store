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
#include <optional>
#include <vector>
#include <filesystem>

namespace kv {

struct SSTableMeta {
    std::string filename;
    std::string min_key, max_key;
};

class SSTableReader {
public:
    explicit SSTableReader(const std::string& data_dir);

    // Scan SSTables newest -> oldest
    std::optional<std::string> get(const std::string& key) const;

private:
    // Scan and build SSTables _tables[] cache (newest first)
    void loadAllTables();

    // Read from a single SSTable file
    std::optional<std::string>
    readOneSSTable(const SSTableMeta& sstable_meta, const std::string& key) const;

    std::string _data_dir;
    std::vector<SSTableMeta> _tables;
};

} // namespace kv

