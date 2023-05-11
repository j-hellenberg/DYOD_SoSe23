#include "storage_manager.hpp"

#include <utility>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  // Will refer to the same instance on all method invocations and can therefore be used as a singleton.
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  const auto insertion_successful = _tables.insert({name, table}).second;
  Assert(insertion_successful, "Table '" + name + "' already exists.");
}

void StorageManager::drop_table(const std::string& name) {
  const auto deleted_elements = _tables.erase(name);
  Assert(deleted_elements != 0, "Cannot drop non-existing table '" + name + "'.");
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  Assert(has_table(name), "Cannot find table.");
  return _tables.at(name);
}

bool StorageManager::has_table(const std::string& name) const {
  return _tables.contains(name);
}

std::vector<std::string> StorageManager::table_names() const {
  auto names = std::vector<std::string>{};
  names.reserve(_tables.size());

  for (const auto& [name, table] : _tables) {
    names.emplace_back(name);
  }

  return names;
}

void StorageManager::print(std::ostream& out) const {
  out << "Tables managed by the storage manager:" << std::endl;
  for (const auto& table : _tables) {
    out << table.first;
    out << " (";
    out << table.second->column_count() << " column(s), ";
    out << table.second->row_count() << " row(s), ";
    out << table.second->chunk_count() << " chunk(s)";
    out << ")" << std::endl;
  }
}

void StorageManager::reset() {
  _tables.clear();
  // Clearing the map did only remove its entries, not change its capacity. Therefore, perform a rehash so that we
  // can free up unused capacity.
  _tables.rehash(0);
}

}  // namespace opossum
