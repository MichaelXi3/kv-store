#include "kv/sstable_reader.hpp"
#include <fstream>
#include <iostream> // Added for logging

namespace kv {

// Constructor: scan all SSTables and build SSTableMeta vector
SSTableReader::SSTableReader(const std::string& data_dir, std::shared_ptr<LockManager> lock_mgr) 
    : _data_dir(data_dir), _lock_mgr(lock_mgr)
{
    loadAllTables();
}

// Scan data_dir for *.sst files and record min/max key
// Sort newest->oldest and cache in _tables
void
SSTableReader::loadAllTables()
{
    namespace fs = std::filesystem;
    _tables.clear();

    // Iterate all sstables, build SSTableMeta for each and push to _tables
    for (auto& entry : fs::directory_iterator(_data_dir)) {
        if (entry.path().extension() != ".sst")  continue;

        SSTableMeta meta;
        meta.filename = entry.path().filename().string();

        // Read one sst file, find min/max key
        std::ifstream in(entry.path(), std::ios::binary);
        if (!in.is_open()) continue;

        // Parse key value pairs
        bool first_key_not_locate = true;
        while (true) {
            uint32_t key_len;
            if (!in.read(reinterpret_cast<char *>(&key_len), sizeof(key_len)))
                break;
            
            // Get the key value
            std::string key(key_len, '\0');
            in.read(key.data(), key_len); // fills the string with key data

            // Skip the value bytes
            uint32_t value_len;
            in.read(reinterpret_cast<char *>(&value_len), sizeof(value_len));
            in.seekg(value_len, std::ios::cur);

            if (first_key_not_locate) {
                meta.max_key = meta.min_key = key;
                first_key_not_locate = false;
            } else {
                if (key > meta.max_key) meta.max_key = key;
                if (key < meta.min_key) meta.min_key = key;
            }

        }

        // This sstable has at least one key value pair
        if (!first_key_not_locate) {
            _tables.push_back(std::move(meta));
        }
    }

    // Sort the _tables in order of newest->oldest based on filename
    std::sort(_tables.begin(),
              _tables.end(),
              [](auto &a, auto &b){
                return a.filename > b.filename;
              });
}

std::optional<std::string>
SSTableReader::get(const std::string& key) const
{
    auto lock = _lock_mgr->acquireSSTableReadLock();

    std::cout << "DEBUG: SSTableReader::get() - Searching for key '" << key << "' across " << _tables.size() << " SSTables" << std::endl;
    
    // Search through all SSTables metas from newest to oldest
    for (const auto& table : _tables) {
        // Check if the key could be in this SSTable based on min/max key range
        if (key >= table.min_key && key <= table.max_key) {
            std::cout << "DEBUG: SSTableReader::get() - Key might be in SSTable: " << table.filename 
                      << " (range: " << table.min_key << " - " << table.max_key << ")" << std::endl;
            
            if (auto result = readOneSSTable(table, key)) {
                std::cout << "DEBUG: SSTableReader::get() - Found key '" << key << "' in SSTable: " << table.filename << std::endl;
                return result;
            }
        }
    }
    
    std::cout << "DEBUG: SSTableReader::get() - Key '" << key << "' not found in any SSTable" << std::endl;
    return std::nullopt;
}

// Public method to refresh metadata (called after compaction/flush)
void SSTableReader::refreshMetadata() {
    std::cout << "DEBUG: SSTableReader::refreshMetadata() - Reloading SSTable metadata" << std::endl;
    loadAllTables();
    std::cout << "DEBUG: SSTableReader::refreshMetadata() - Loaded " << _tables.size() << " SSTable files" << std::endl;
}

std::optional<std::string>
SSTableReader::readOneSSTable(const SSTableMeta& sstable_meta,
                              const std::string& key) const
{
    std::string sstable_filepath = _data_dir + "/" + sstable_meta.filename;
    std::ifstream in(sstable_filepath, std::ios::binary);
    
    if (!in.is_open()) {
        std::cout << "DEBUG: SSTableReader::readOneSSTable() - Failed to open SSTable: " << sstable_filepath << std::endl;
        return std::nullopt;
    }
    
    std::cout << "DEBUG: SSTableReader::readOneSSTable() - Scanning SSTable: " << sstable_meta.filename << " for key: " << key << std::endl;
    
    // Read through all key-value pairs in the SSTable
    while (true) {
        uint32_t key_len;
        if (!in.read(reinterpret_cast<char*>(&key_len), sizeof(key_len))) {
            break; // End of file
        }
        
        // Read the key
        std::string current_key(key_len, '\0');
        in.read(current_key.data(), key_len);
        
        // Read value length
        uint32_t value_len;
        in.read(reinterpret_cast<char*>(&value_len), sizeof(value_len));
        
        if (current_key == key) {
            // Found the key, read and return the value
            std::string value(value_len, '\0');
            in.read(value.data(), value_len);
            std::cout << "DEBUG: SSTableReader::readOneSSTable() - Found matching key in SSTable" << std::endl;
            return value;
        } else {
            // Skip the value bytes
            in.seekg(value_len, std::ios::cur);
        }
    }
    
    std::cout << "DEBUG: SSTableReader::readOneSSTable() - Key not found in SSTable: " << sstable_meta.filename << std::endl;
    return std::nullopt;
}

}