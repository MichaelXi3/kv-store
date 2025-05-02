#include "kv/file_handle.hpp"
#include <fstream>
#include <iostream>

namespace kv {

// Constructor: Opens the file in append mode
FileHandle::FileHandle(const std::string& filePath) 
    : _filePath {filePath} {
    _file.open(filePath, std::ios::app); // Open file in append mode
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