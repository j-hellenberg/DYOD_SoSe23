#include "storage_manager.hpp"

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  // Will refer to the same instance on all method invocations and can therefore be used as a singleton.
  static StorageManager INSTANCE;
  return INSTANCE;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  _table_names.emplace_back(name);
  _tables.emplace_back(table);
}

void StorageManager::drop_table(const std::string& name) {
  Assert(has_table(name), "Cannot drop non-existing table.");
  auto index = find(_table_names.begin(), _table_names.end(), name);

  _table_names.erase(index);
  _tables.erase(_tables.begin() + (index - _table_names.begin()));
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  Assert(has_table(name), "Cannot find table.");
  auto index = find(_table_names.begin(), _table_names.end(), name);

  return _tables.at(index - _table_names.begin());
}

bool StorageManager::has_table(const std::string& name) const {
  auto index = find(_table_names.begin(), _table_names.end(), name);
  return index != _table_names.end();
}

std::vector<std::string> StorageManager::table_names() const {
  return _table_names;
}

void StorageManager::print(std::ostream& out) const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

void StorageManager::reset() {
  _table_names.clear();
  _tables.clear();
}

}  // namespace opossum
