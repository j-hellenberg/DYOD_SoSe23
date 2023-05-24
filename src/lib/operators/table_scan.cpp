#include "table_scan.hpp"
#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "utils/assert.hpp"

namespace opossum {
TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value) : AbstractOperator(in, nullptr),
_column_id(column_id), _scan_type(scan_type), _search_value(search_value) {
}

ColumnID TableScan::column_id() const {
  return _column_id;
}

ScanType TableScan::scan_type() const {
  return _scan_type;
}

const AllTypeVariant& TableScan::search_value() const {
  return _search_value;
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  // TODO(student) implement it in a source file and change this to a declaration.
  Fail("Implementation missing.");
}
}  // namespace opossum
