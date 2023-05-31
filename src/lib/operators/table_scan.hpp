#pragma once

#include <unordered_map>
#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "utils/assert.hpp"

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
  std::unordered_map<AbstractSegment*, std::function<bool(ChunkOffset)>> _filter_functions;

  std::shared_ptr<const Table> _on_execute() override;

  // Return the rows of a chunk matching the filter of this scan.
  // Note that this method does not return RowIDs, but the ChunkOffset within the segment, which needs to be then
  // converted to RowIDs in the output segments.
  // Note that this differentiation is necessary, because a ChunkOffset may map to different RowIDs in different
  // ReferenceSegments.
  std::shared_ptr<const std::vector<ChunkOffset>> _filter(const std::string& column_type,
                                                          const std::shared_ptr<const Chunk> chunk);

  // Obtain a function that can decide for the given segment whether a ChunkOffset matches the filter of this scan.
  std::function<bool(ChunkOffset)> _filter_function_for_segment(const std::string& column_type,
                                                                const std::shared_ptr<AbstractSegment>& target_segment);

  // Return true iff the input_table is a materialized table (e.g., one that is not consisting of reference segments
  // pointing to other tables).
  bool _input_table_is_materialized();
};

}  // namespace opossum
