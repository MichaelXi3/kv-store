#include "kv/flusher.hpp"
#include <chrono>
#include <map>

namespace kv {

Flusher::Flusher(std::shared_ptr<MemTable>& active_table,
                 std::mutex&               active_table_mutex,
                 std::mutex&               immu_table_mutex,
                 SSTableWriter&            writer,
                 uint64_t                  threshold)
    : active_table(active_table)
    , active_table_mutex(active_table_mutex)
    , writer(writer)
    , threshold(threshold)
    , running(false)
    , immu_table_mutex(immu_table_mutex)
    , next_file_number(1)
{}

Flusher::~Flusher() {
    stop();
}

// start the flusher thread
void Flusher::start() {
    running.store(true);
    bg_flusher_thread = std::thread(&Flusher::run, this);
}

void Flusher::stop() {
    running.store(false);
    immu_cv.notify_all();
    if (bg_flusher_thread.joinable()) {
        bg_flusher_thread.join();
    }
}

void Flusher::run() {
    while (running.load()) {
        bool should_flush = false;
        // Step 1: Check and swap active to immu_table
        {
            std::lock_guard lk(active_table_mutex);
            // check if memstable flush is needed
            if (active_table->size() >= threshold) {
                // freeze the active_table
                {
                    std::lock_guard lk2(immu_table_mutex);
                    immutable_table = active_table;
                } // drop immu_table_mutex
                // redirect write to new table
                active_table = std::make_shared<MemTable>();
                should_flush = true;
                immu_cv.notify_one();
            } // drop active_table_mutex
        }
        // Step 2: Flush the immutable if there is one
        if (should_flush) {
            std::shared_ptr<MemTable> table_to_flush;
            {
                std::lock_guard lk(immu_table_mutex);
                table_to_flush = immutable_table;
            }
            if (table_to_flush) {
                // sort the table first
                std::map<std::string, std::string> sorted_data;
                for (auto& [k, v] : table_to_flush->data()) {
                    sorted_data.emplace(k, v);
                }
                // write SSTable
                uint64_t sst_file_no = next_file_number.fetch_add(1);
                writer.writeSSTable(sorted_data, sst_file_no);

                // relase the immu_table
                {
                    std::lock_guard lk(immu_table_mutex);
                    immutable_table.reset();
                }
            }
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // Final cleanup in case anything is left before stop
    {
        std::lock_guard lk(immu_table_mutex);
        if (immutable_table) {
            std::map<std::string, std::string> sorted_data;
            for (auto& [k, v] : immutable_table->data())
                sorted_data.emplace(k, v);

            uint64_t sst_file_no = next_file_number.fetch_add(1);
            writer.writeSSTable(sorted_data, sst_file_no);
            immutable_table.reset();
        }
    }  
}

} // kv namespace