#pragma once
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <map>
#include <cstdint>
#include "kv/lock_manager.hpp"

namespace kv {

// Forward declaration
class KVStore;

class Compactor {
public:
    Compactor(const std::string& data_dir,
              size_t threshold,        // Compaction trigger threshold - sstable counts
              size_t compaction_count, // For each round of compaction, compact this cnt of tables
              std::shared_ptr<LockManager> lock_mgr);
    ~Compactor();

    void start();
    void stop();
    
    // Set KVStore reference after both objects are constructed (breaks circular dependency)
    void setKVStore(KVStore* kv_store);

private:
    void                run();                    // Compactor trigger function call
    std::vector<std::string> discoverSSTables();  // Get the list of sstables
    void                performCompaction(const std::vector<std::string>& sstables); // Meat of compaction
    // Helper for performCompaction
    std::map<std::string, std::string> performMultiWayMerge(const std::vector<std::string>& files); // Multi-way merge
    uint64_t            generateNewFileNumber();  // Generate new file number for compacted SSTable
    
    std::string         _data_dir;                // Root path of KV store
    size_t              _trigger_threshold;
    size_t              _compaction_count;
    std::thread         _thread;
    std::atomic<bool>   _is_running;
    std::shared_ptr<LockManager> _lock_mgr;
    KVStore*            _kv_store; // Pointer to KVStore for metadata refresh
};

} // namespace kv