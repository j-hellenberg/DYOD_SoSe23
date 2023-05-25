#pragma once

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "utils/assert.hpp"
#include "storage/chunk.hpp"
#include <unordered_map>

namespace opossum {

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ColumnID column_id() const;

  ScanType scan_type() const;

  const AllTypeVariant& search_value() const;

 protected:
  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;

  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<const PosList> _filter(std::string& row_value, std::shared_ptr<const Chunk> chunk, ChunkID& chunk_id);

  std::function<bool(ChunkOffset)> _filter_function_for_segment(std::string& column_type, std::shared_ptr<AbstractSegment>& target_segment);
  std::unordered_map<AbstractSegment*, std::function<bool(ChunkOffset)>> _filter_functions;

};

}  // namespace opossum
