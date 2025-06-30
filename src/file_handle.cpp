#include "kv/file_handle.hpp"
#include <fstream>
#include <iostream>
#include <filesystem>

namespace kv {

// Constructor: Opens the file in append mode
FileHandle::FileHandle(const std::string& filePath) 
    : _filePath {filePath} {
    
    std::cout << "DEBUG: FileHandle created for: " << filePath << std::endl;
    
    // Check if directory exists
    std::filesystem::path file_path(filePath);
    std::filesystem::path parent_dir = file_path.parent_path();
    
    if (!std::filesystem::exists(parent_dir)) {
        std::cout << "DEBUG: Race! Parent db directory doesn't exist yet: " << parent_dir << std::endl;
        std::filesystem::create_directories(parent_dir);
        std::cout << "DEBUG: Created parent directory" << std::endl;
    }
    
    _file.open(filePath, std::ios::app); // Open file in append mode
    std::cout << "DEBUG: File is " << (_file.is_open() ? "OPEN" : "CLOSED") << std::endl;
    
    if (!_file.is_open()) {
        std::cout << "DEBUG: Failed to open file. Error flags: " << std::endl;
        std::cout << "  failbit: " << _file.fail() << std::endl;
        std::cout << "  badbit: " << _file.bad() << std::endl;
        std::cout << "  eofbit: " << _file.eof() << std::endl;
    }
};

// Destructor: Closes the file
FileHandle::~FileHandle() {
    if (_file.is_open()) {
        _file.close();
    }
};

// Returns the ofstream reference
std::ofstream& FileHandle::get() {
    return _file;
};

// Writes data to the file and flushes it
void FileHandle::write(const std::string &data) {
    if (_file.is_open()) {
        _file << data;
        _file.flush();
    } else {
        std::cerr << "Error: File is not open for writing." << std::endl;
    }
}

// Prints the content of the file
void FileHandle::printContent() const {
    std::ifstream inFile {_filePath};
    if (!inFile.is_open()) {
        std::cerr << "Error opening file: " << _filePath << std::endl;
        return;
    }
    std::string line;
    while (std::getline(inFile, line)) {
        std::cout << line << std::endl;
    }
}

} // namespace kv