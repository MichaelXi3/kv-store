/**
 * @file log_writter.hpp
 *
 * LogWriter is a class that manages the writing of Write-Ahead Log (WAL)
 * Callers just call appendRecord() without worrying about the details of
 * file mode or flush logic.
 * 
 * Multiple threads can safely call appendRecord() concurrently, so mutex
 * is used to protect the file stream.
 *
 * Typical usage:
 *
 * Used by:
 *  - MemTable: to write WAL records
 */

#include "kv/file_handle.hpp"
#include <mutex>
#include <string>

namespace kv {

class LogWriter {
public:
    explicit LogWriter(const std::string& filePath);  // Constructor
    ~LogWriter();                                     // Destructor

    void appendRecord(const std::string& record);     // Appends a record to the WAL log

private:
    FileHandle _fileHandle;                           // File handle for writing
    std::mutex _mutex;                                // Mutex for thread-safe access
};

} // namespace kv