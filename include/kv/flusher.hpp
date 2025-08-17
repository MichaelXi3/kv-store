/*
 * Flusher
 * - Monitor the active memtable size, and once filled, switch to the new memtable
 * - Meanwhile, freeze the old memtable, then sort it and send to SSTablewriter
 * - Lastly, delete the old WAL, and continue to monitor the next memtable
 */
#pragma once
#include <thread>
#include "kv/memtable.hpp"
#include "kv/sstable_writer.hpp"
#include "kv/lock_manager.hpp"

namespace kv {

class Flusher {
public:
    Flusher(std::shared_ptr<MemTable>& active_table,
            std::mutex& active_table_mutex,
            std::mutex& immu_table_mutex,
            SSTableWriter& writer,
            uint64_t threshold,
            std::shared_ptr<LockManager> lock_mgr);
    
    ~Flusher();

    // start the monitor thread
    void start();
    
    // stop the monitor thread, after current flush done
    void stop();

private:
    void run();

    std::shared_ptr<MemTable>& active_table;
    std::mutex& active_table_mutex;

    SSTableWriter& writer;
    uint64_t threshold; // threshold to switch table

    std::thread bg_flusher_thread;
    std::atomic<bool> running; // current state of thread running

    std::shared_ptr<MemTable> immutable_table;
    std::mutex& immu_table_mutex;
    std::condition_variable immu_cv;

    std::atomic<uint64_t> next_file_number;

    std::shared_ptr<LockManager> lock_mgr;
};

}