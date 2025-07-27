#pragma once
#include "kv/memtable.hpp"
#include "kv/file_handle.hpp"
#include "kv/log_writer.hpp"
#include "kv/sstable_reader.hpp"

namespace kv {

class KVStore {
    public:
        // constructor and destructor
        explicit KVStore(const std::string& db_path);
        ~KVStore();

        // Durably write by WAL + in-memory insert
        void put(const std::string& key, const std::string& value);

        // Look up value based on key
        // - First look-up from in-memory lookup MemTable
        // - Second look-up from persistent sstables (TODO)
        std::optional<std::string> get(const std::string& key);

    private:
        std::string _db_path;
        std::string _wal_path;
        LogWriter _wal;
        MemTable _memtable;
        SSTableReader _reader;

        void replayWAL();
};

}
