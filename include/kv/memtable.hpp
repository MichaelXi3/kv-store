/**
 * @file memtable.hpp
 * @brief A simple in-memory key-value store implementation.
 *
 * MemTable is a basic in-memory key-value store that uses an unordered_map
 * to store key-value pairs. It provides thread-safe put and get operations.
 */

#pragma once
#include <iostream>
#include <unordered_map>

namespace kv {

class MemTable {
public:
    MemTable();
    ~MemTable() = default;

    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;

    size_t size() const;

private:
    std::unordered_map<std::string, std::string> _map; // Hashmap to store key-value pairs
};

} // namespace kv 