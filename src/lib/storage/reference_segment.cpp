#include "reference_segment.hpp"

#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table>& referenced_table,
                                   const ColumnID referenced_column_id, const std::shared_ptr<const PosList>& pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _position_list(pos) {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  Assert(chunk_offset < _position_list->size(), "Invalid offset given for chunk: " + std::to_string(chunk_offset));
  auto position = _position_list->operator[](chunk_offset);

  if (position.is_null()) {
    return NULL_VALUE;
  }

  auto referenced_segment = _referenced_table->get_chunk(position.chunk_id)->get_segment(_referenced_column_id);
  return referenced_segment->operator[](position.chunk_offset);
}

ChunkOffset ReferenceSegment::size() const {
  return _position_list->size();
}

const std::shared_ptr<const PosList>& ReferenceSegment::pos_list() const {
  return _position_list;
}

const std::shared_ptr<const Table>& ReferenceSegment::referenced_table() const {
  return _referenced_table;
}

ColumnID ReferenceSegment::referenced_column_id() const {
  return _referenced_column_id;
}

size_t ReferenceSegment::estimate_memory_usage() const {
  return _position_list->capacity() * sizeof(RowID);
}

}  // namespace opossum
