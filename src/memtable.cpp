#include "kv/memtable.hpp"
#include <iostream>
#include <unordered_map>
#include <optional>

namespace kv {

MemTable::MemTable() = default;

void
MemTable::put(const std::string& key, const std::string& value) {
    _map[key] = value;
}

// An optional means "there may or may not be a value" without resorting to 
// tricks returning an empty string or throwing an exception every time a key is missing.
std::optional<std::string>
MemTable::get(const std::string& key) const {
    auto pair = _map.find(key);
    if (pair == _map.end()) {
        return std::nullopt;
    }
    return pair->second;
}

size_t MemTable::size() const {
    return _map.size();
}

const std::unordered_map<std::string, std::string>&
MemTable::data() const {
    return _map;
}

}