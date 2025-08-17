#include "kv/compactor.hpp"
#include "kv/sstable_writer.hpp"
#include "kv/kv_store.hpp"
#include <iostream>
#include <filesystem>
#include <algorithm>
#include <vector>
#include <fstream>
#include <map>
#include <queue>
#include <memory>
#include <iomanip>

namespace kv {

// Helper struct for multi-way merge
struct SSTableIterator {
    std::ifstream file;
    std::string current_key;
    std::string current_value;
    bool is_valid;
    std::string filename;
    size_t file_age; // Track file age for conflict resolution (higher = newer)
    
    SSTableIterator(const std::string& filepath)
        : is_valid(false),
          filename(filepath),
          file_age(0)
    {
        file.open(filepath, std::ios::binary);
        if (file.is_open()) {
            advance(); // Read first key-value pair
        }
    }
    
    void advance() {
        if (!file.is_open()) {
            is_valid = false;
            return;
        }
        
        uint32_t key_len;
        if (!file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len))) {
            is_valid = false;
            return;
        }
        
        current_key.resize(key_len);
        if (!file.read(current_key.data(), key_len)) {
            is_valid = false;
            return;
        }
        
        uint32_t value_len;
        if (!file.read(reinterpret_cast<char*>(&value_len), sizeof(value_len))) {
            is_valid = false;
            return;
        }
        
        current_value.resize(value_len);
        if (!file.read(current_value.data(), value_len)) {
            is_valid = false;
            return;
        }
        
        is_valid = true;
    }
};

// Comparator for priority queue (min-heap by key, then by file age)
struct IteratorComparator {
    bool operator()(const std::shared_ptr<SSTableIterator>& a, 
                    const std::shared_ptr<SSTableIterator>& b) const {
        // Primary sort: by key (alphabetical order)
        if (a->current_key != b->current_key) {
            return a->current_key > b->current_key; // Min-heap: smaller keys first
        }
        
        // Secondary sort: for same key, process older files first (smaller file_age)
        // This ensures older values are seen first and can be overridden by newer ones
        return a->file_age > b->file_age; // Min-heap: older files (smaller age) first
    }
};

// Constructor
Compactor::Compactor(const std::string& data_dir, size_t threshold, size_t compaction_count, std::shared_ptr<LockManager> lock_mgr)
    : _data_dir(data_dir),
      _trigger_threshold(threshold),
      _compaction_count(compaction_count),
      _is_running(false),
      _lock_mgr(lock_mgr),
      _kv_store(nullptr)
{
    std::cout << "DEBUG: Compactor created - data_dir: " << data_dir 
              << ", threshold: " << threshold 
              << ", compaction_count: " << compaction_count << std::endl;
}

// Destructor
Compactor::~Compactor() {
    if (_is_running.load()) {
        stop();
    }
}

// Start the compaction thread
void Compactor::start() {
    if (_is_running.load()) {
        std::cout << "DEBUG: Compactor already running" << std::endl;
        return;
    }
    
    _is_running.store(true);
    _thread = std::thread(&Compactor::run, this);
    std::cout << "DEBUG: Compactor thread started" << std::endl;
}

// Stop the compaction thread
void Compactor::stop() {
    if (!_is_running.load()) {
        std::cout << "DEBUG: Compactor already stopped" << std::endl;
        return;
    }
    
    _is_running.store(false);
    if (_thread.joinable()) {
        _thread.join();
    }
    std::cout << "DEBUG: Compactor thread stopped" << std::endl;
}

// Main compaction loop - similar to Flusher
void Compactor::run() {
    while (_is_running.load()) {
        try {
            // Step 1: Discover all SSTable files
            std::vector<std::string> sstable_files = discoverSSTables();
            
            // Step 2: Check if compaction is needed
            if (sstable_files.size() >= _trigger_threshold) {
                std::cout << "DEBUG: Compaction triggered - found " << sstable_files.size() 
                          << " SSTables, threshold: " << _trigger_threshold << std::endl;
                
                // Step 3: Select files to compact (oldest N files)
                size_t file_cnt_to_compact = std::min(_compaction_count, sstable_files.size());
                std::vector<std::string> files_for_compaction(
                    sstable_files.begin(), 
                    sstable_files.begin() + file_cnt_to_compact
                );
                
                std::cout << "DEBUG: Selected " << files_for_compaction.size() 
                          << " files for compaction" << std::endl;
                
                // Perform actual compaction
                performCompaction(files_for_compaction);
            }
        } catch (const std::exception& e) {
            std::cerr << "ERROR: Compaction failed: " << e.what() << std::endl;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    std::cout << "DEBUG: Compactor run loop exiting" << std::endl;
}

// Set KVStore reference (called after both objects are constructed)
void Compactor::setKVStore(KVStore* kv_store) {
    _kv_store = kv_store;
    std::cout << "DEBUG: Compactor linked to KVStore for metadata refresh" << std::endl;
}

// Discover all SSTable files in data directory, sorted by filename (oldest first)
std::vector<std::string> Compactor::discoverSSTables() {
    std::vector<std::string> sstable_files;
    
    try {
        namespace fs = std::filesystem;
        
        // Scan data directory for .sst files
        for (const auto& entry : fs::directory_iterator(_data_dir)) {
            if (entry.path().extension() == ".sst") {
                sstable_files.push_back(entry.path().filename().string());
            }
        }
        
        // Sort by filename (E.g. 00000001.sst, 00000002.sst, etc.)
        // This gives us oldest first (lower numbers = older)
        std::sort(sstable_files.begin(), sstable_files.end());
        
        std::cout << "DEBUG: Discovered " << sstable_files.size() << " SSTable files" << std::endl;
        
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "ERROR: Failed to scan directory " << _data_dir << ": " << e.what() << std::endl;
    }
    
    return sstable_files;
}

// Meat of the compaction logic
void Compactor::performCompaction(const std::vector<std::string>& files) {
    std::cout << "DEBUG: performCompaction called with " << files.size() << " files" << std::endl;
    if (files.empty()) {
        std::cout << "DEBUG: No files to compact" << std::endl;
        return;
    }
    for (const auto& file : files) {
        std::cout << "DEBUG: Compacting file: " << file << std::endl;
    }

    try {
        // 1. Acquire SSTable write lock (blocks if flusher is active)
        std::cout << "DEBUG: Acquiring SSTable write lock..." << std::endl;
        auto sstable_lock = _lock_mgr->acquireSSTableWriteLock();
        std::cout << "DEBUG: SSTable write lock acquired" << std::endl;

        // 2. Multi-way merge of selected files
        std::cout << "DEBUG: Starting multi-way merge..." << std::endl;
        std::map<std::string, std::string> merged_data = performMultiWayMerge(files);
        std::cout << "DEBUG: Multi-way merge completed. Merged " << merged_data.size() << " key-value pairs" << std::endl;

        // 3. Write new compacted SSTable
        std::cout << "DEBUG: Writing compacted SSTable..." << std::endl;
        uint64_t new_file_number = generateNewFileNumber();
        SSTableWriter writer(_data_dir);
        
        if (!writer.writeSSTable(merged_data, new_file_number)) {
            throw std::runtime_error("Failed to write compacted SSTable");
        }
        
        std::cout << "DEBUG: Compacted SSTable written: " << std::setfill('0') << std::setw(8) << new_file_number << ".sst" << std::endl;

        // 4. Delete old files
        std::cout << "DEBUG: Deleting old SSTable files..." << std::endl;
        for (const auto& filename : files) {
            std::string old_sstable_path = _data_dir + "/" + filename;
            if (std::filesystem::exists(old_sstable_path)) {
                std::filesystem::remove(old_sstable_path);
                std::cout << "DEBUG: Deleted old file: " << filename << std::endl;
            }
        }

        // 5. Refresh SSTable metadata in KVStore (while still holding lock)
        if (_kv_store) {
            std::cout << "DEBUG: Refreshing SSTable metadata after compaction..." << std::endl;
            _kv_store->refreshSSTableMetadata();
        }

        std::cout << "DEBUG: Compaction completed successfully!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "ERROR: Compaction failed: " << e.what() << std::endl;
    }
}

// Perform multi-way merge of SSTable files
std::map<std::string, std::string>
Compactor::performMultiWayMerge(const std::vector<std::string>& files) {
    std::map<std::string, std::string> merged_data; // Compacted SSTable data
    
    // Create iterators for all input files
    std::priority_queue<std::shared_ptr<SSTableIterator>,              // What are in the min heap?   
                        std::vector<std::shared_ptr<SSTableIterator>>, // Container of iterators
                        IteratorComparator> min_heap;                  // Comparator for min-heap
    
    // Initialize iterators and add valid ones to heap
    // NOTE: files are sorted oldest->newest, so we assign file_age accordingly
    for (size_t file_idx = 0; file_idx < files.size(); ++file_idx) {
        const auto& filename = files[file_idx];
        std::string sstable_path = _data_dir + "/" + filename;
        // Create an iterator for each sstable, and at this point, the iterator points to the first key-value pair
        auto iterator = std::make_shared<SSTableIterator>(sstable_path);
        iterator->file_age = file_idx; // Track file age (higher index = newer file)
        
        if (iterator->is_valid) {
            min_heap.push(iterator);
            // Higher file_idx means newer file
            std::cout << "DEBUG: Added iterator for file: " << filename << " (age: " << file_idx << ")" << std::endl;
        } else {
            std::cout << "DEBUG: Failed to open file: " << filename << std::endl;
        }
    }
    
    // Multi-way merge using min-heap
    // Based on the comparator, we process:
    // 1. Keys in alphabetical order
    // 2. For duplicate keys, older files first, then newer files (which override)
    while (!min_heap.empty()) {
        // Get the iterator with the smallest key (and oldest file for duplicate keys)
        auto current_iter = min_heap.top();
        min_heap.pop();
        
        std::string key = current_iter->current_key;
        std::string value = current_iter->current_value;
        
        // Simple override logic: later values automatically override earlier ones
        // because the comparator ensures older files are processed first
        if (merged_data.find(key) != merged_data.end()) {
            std::cout << "DEBUG: Duplicate key '" << key << "' - overriding with newer value from file age " 
                      << current_iter->file_age << std::endl;
        }
        merged_data[key] = value;

        // Handle tombstone
        if (value == TOMB_STONE) {
            merged_data.erase(key);
            std::cout << "DEBUG: Removed deleted key: " << key << std::endl;
        }

        // Advance the iterator and add back to heap if still valid
        current_iter->advance();
        if (current_iter->is_valid) {
            min_heap.push(current_iter);
        }
    }
    
    return merged_data;
}

// Generate a new file number for the compacted SSTable
uint64_t Compactor::generateNewFileNumber() {
    uint64_t max_number = 0;
    
    try {
        // Scan directory to find the highest existing file number
        for (const auto& entry : std::filesystem::directory_iterator(_data_dir)) {
            if (entry.path().extension() == ".sst") {
                // stem() means the file name without the extension
                std::string filename = entry.path().stem().string();
                try {
                    // stoull() converts the string to uint64_t
                    uint64_t file_number = std::stoull(filename);
                    max_number = std::max(max_number, file_number);
                } catch (const std::exception&) {
                    // Ignore files with non-numeric names
                }
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "ERROR: Failed to scan directory for file numbers: " << e.what() << std::endl;
    }
    
    return max_number + 1;
}

} // namespace kv
