#pragma once

#include "abstract_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

// Operator to retrieve a table from the StorageManager by specifying its name.
class GetTable : public AbstractOperator {
 public:
  explicit GetTable(const std::string& name);

  // Returns the name of the table we are referring to.
  const std::string& table_name() const;

 protected:
  std::string _table_name;
  std::shared_ptr<const Table> _on_execute() override;
};

}  // namespace opossum
