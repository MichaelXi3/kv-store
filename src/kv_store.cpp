#include "kv/kv_store.hpp"
#include <sstream>
#include <iostream>

namespace kv {

// Constructor
KVStore::KVStore(const std::string& db_path)
    : _db_path {db_path},
      _wal_path {db_path + "/wal.log"},     // WAL inside the directory
      _wal {_wal_path},
      _memtable {},
      _reader {db_path}
{
    // (1) Create db directory if it doesn't exist
    std::filesystem::create_directories(db_path);

    // (2) SSTables min/max key indexes are automatically loaded in SSTableReader constructor
    
    // (3) Replay WAL to restore in-memory state
    std::cout << "DEBUG: KVStore created with WAL path: " << _wal_path << std::endl;
    replayWAL();
}

KVStore::~KVStore() = default;

// Write to KVStore, first append to WAL, then insert into MemTable
void KVStore::put(const std::string& key, const std::string& value) {
    std::string record = key + " " + value + "\n";
    std::cout << "DEBUG: KVStore::put() - Writing to WAL: '" << record.substr(0, record.length()-1) << "'" << std::endl;
    
    // durable write by WAL first
    _wal.appendRecord(record);   
    // in-memory insert to MemTable
    _memtable.put(key, value);
}

// In-memory lookup MemTable
std::optional<std::string> KVStore::get(const std::string& key) {
    // Search in-memory hash table
    if (auto v = _memtable.get(key)) {
        std::cout << "DEBUG: KVStore::get() - Found key '" << key << "' in in-memory hash table" << std::endl;
        return v;
    }
    
    std::cout << "DEBUG: KVStore::get() - Key '" << key << "' not found in memory, scanning on-disk SSTables" << std::endl;
    // Fall back to SSTables read
    return _reader.get(key);
}

// Replay WAL to restore in-memory state
void KVStore::replayWAL() {
    std::cout << "DEBUG: Attempting to replay WAL from: " << _wal_path << std::endl;
    
    std::ifstream wal_file(_wal_path);
    
    std::string line;
    int line_count = 0;
    while (std::getline(wal_file, line)) {
        line_count++;
        std::cout << "DEBUG: Read WAL line " << line_count << ": '" << line << "'" << std::endl;
        
        std::istringstream iss(line);
        std::string key, value;
        if (iss >> key >> value) {
            std::cout << "DEBUG: Replaying - Key: '" << key << "', Value: '" << value << "'" << std::endl;
            _memtable.put(key, value);
        }
    }
    std::cout << "DEBUG: WAL replay completed, processed " << line_count << " lines" << std::endl;
}

}