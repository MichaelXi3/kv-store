#pragma once
#include "kv/memtable.hpp"
#include "kv/file_handle.hpp"
#include "kv/log_writer.hpp"
#include "kv/sstable_reader.hpp"
#include "kv/lock_manager.hpp"

namespace kv {

const std::string TOMB_STONE = "__TOMBSTONE__";

class KVStore {
    public:
        // constructor and destructor
        explicit KVStore(const std::string& db_path, std::shared_ptr<LockManager> lock_mgr);
        ~KVStore();

        // Durably write by WAL + in-memory insert
        void put(const std::string& key, const std::string& value);

        // Look up value based on key
        // - First look-up from in-memory lookup MemTable
        // - Second look-up from persistent sstables (TODO)
        std::optional<std::string> get(const std::string& key);
        
        // Delete a key by placing a tombstone
        void del(const std::string& key);
        
        // Refresh SSTable metadata (called after compaction)
        void refreshSSTableMetadata();

    private:
        std::string _db_path;
        std::string _wal_path;
        LogWriter _wal;
        MemTable _memtable;
        SSTableReader _reader;
        std::shared_ptr<LockManager> _lock_mgr;

        void replayWAL();
};

}
