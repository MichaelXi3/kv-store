/**
 * LockManager - Centralized Lock Coordination for KV Store Components
 * 
 * The LockManager provides a single point of control for all locking operations
 * across the KV store system, ensuring proper coordination between components
 * and preventing deadlocks through consistent lock ordering.
 * 
 * Usage Pattern:
 *   auto lock_mgr = std::make_shared<LockManager>();
 *   KVStore store(db_path, lock_mgr);
 *   Flusher flusher(memtable, mutexes, writer, threshold, lock_mgr);
 *   - All components now coordinate through the same lock manager
 * 
 * Lock Types:
 * - SSTable Read Lock: Shared lock for concurrent read operations
 * - SSTable Write Lock: Exclusive lock for SSTable modifications
 * - MemTable Lock: Coordination wrapper for memtable mutex operations
 */
#pragma once
#include <shared_mutex>
#include <memory>
#include <mutex>

namespace kv {

class LockManager {
public:
    // For operations that read SSTable metadata (e.g. get operations, compaction planning)
    std::shared_lock<std::shared_mutex> acquireSSTableReadLock() {
        return std::shared_lock(_sstable_mutex);
    }
    
    // For operations that modify SSTable metadata (e.g. flushing new sstables, compaction)
    std::unique_lock<std::shared_mutex> acquireSSTableWriteLock() {
        return std::unique_lock(_sstable_mutex);
    }
    
    // For memtable operations
    std::lock_guard<std::mutex> acquireMemTableLock(std::mutex& memtable_mutex) {
        return std::lock_guard(memtable_mutex);
    }

private:
    std::shared_mutex _sstable_mutex;
};

} // namespace kv