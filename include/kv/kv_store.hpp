#pragma once
#include "kv/memtable.hpp"
#include "kv/file_handle.hpp"
#include "kv/log_writer.hpp"

namespace kv {

class KVStore {
    public:
        // constructor and destructor
        explicit KVStore(const std::string& db_path);
        ~KVStore();

        // Durably write by WAL + in-memory insert
        void put(const std::string& key, const std::string& value);

        // In-memory lookup MemTable
        std::optional<std::string> get(const std::string& key);

    private:
        std::string _db_path;
        std::string _wal_path;
        LogWriter _wal;
        MemTable _memtable;

        void replayWAL();
};

}
