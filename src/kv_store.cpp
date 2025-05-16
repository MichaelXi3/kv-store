#include "kv/kv_store.hpp"
#include <sstream>

namespace kv {

// Constructor
KVStore::KVStore(const std::string& db_path)
    : _db_path {db_path},
      _wal_path {db_path + ".wal"},
      _wal {_wal_path},
      _memtable {} {
    // Create db directory if it doesn't exist
    std::filesystem::create_directories(db_path);
    // Replay WAL to restore in-memory state
    replayWAL();
}

KVStore::~KVStore() = default;

// Write to KVStore, first append to WAL, then insert into MemTable
void KVStore::put(const std::string& key, const std::string& value) {
    // durable write by WAL
    _wal.appendRecord(key + " " + value + "\n");   
    // in-memory insert to MemTable
    _memtable.put(key, value);
}

// In-memory lookup MemTable
std::optional<std::string> KVStore::get(const std::string& key) {
    return _memtable.get(key);
}

// Replay WAL to restore in-memory state
void KVStore::replayWAL() {
    std::ifstream wal_file(_wal_path);
    std::string line;
    
    while (std::getline(wal_file, line)) {
        // Creates a string stream object iss from the string line. 
        // This extracts words from the line.
        std::istringstream iss(line);
        std::string key, value;
        if (iss >> key >> value) {
            _memtable.put(key, value);
        }
    }
}

}