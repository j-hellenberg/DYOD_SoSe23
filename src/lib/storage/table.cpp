#include "table.hpp"

#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size(target_chunk_size) {
  create_new_chunk();
}

void Table::add_column_definition(const std::string& name, const std::string& type, const bool nullable) {
  _column_names.emplace_back(name);
  _column_types.emplace_back(type);
  _column_nullables.emplace_back(nullable);
}

void Table::add_column(const std::string& name, const std::string& type, const bool nullable) {
  DebugAssert(_chunks.size() <= 1, "It is only possible to add new columns to an empty table.");
  DebugAssert(_column_names.size() < std::numeric_limits<ColumnCount>::max(), "Column limit is already reached.");
  resolve_data_type(type, [this, nullable](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(nullable);
    this->_chunks.at(0)->add_segment(value_segment);
  });
  add_column_definition(name, type, nullable);
}

void Table::create_new_chunk() {
  DebugAssert(_chunks.size() < std::numeric_limits<ChunkID>::max(), "Chunk limit is already reached.");

  const auto chunk = std::make_shared<Chunk>();
  for (auto i = 0; i < column_count(); i++) {
    resolve_data_type(_column_types.at(i), [this, i, &chunk](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(_column_nullables.at(i));
      chunk->add_segment(value_segment);
    });
  }
  _chunks.emplace_back(chunk);
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() >= target_chunk_size()) {
    create_new_chunk();
  }
  _chunks.back()->append(values);
}

ColumnCount Table::column_count() const {
  // Narrowing conversion is ok because we make sure to never have so many columns that the value overflows.
  return static_cast<ColumnCount>(_column_names.size());
}

uint64_t Table::row_count() const {
  uint64_t row_count = 0;
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count(); chunk_id++) {
    row_count += _chunks.at(chunk_id)->size();
  }
  return row_count;
}

ChunkID Table::chunk_count() const {
  // Narrowing conversion is ok because we make sure to never have so many chunks that the value overflows.
  return static_cast<ChunkID>(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto column = find(_column_names.begin(), _column_names.end(), column_name);
  DebugAssert(column != _column_names.end(), "Column with given column name not found.");
  // Narrowing conversion is ok because we make sure to never have so many columns that the value overflows
  return static_cast<ColumnID>(column - _column_names.begin());
}

ChunkOffset Table::target_chunk_size() const {
  return _target_chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  return _column_names;
}

const std::string& Table::column_name(const ColumnID column_id) const {
  return _column_names.at(column_id);
}

const std::string& Table::column_type(const ColumnID column_id) const {
  return _column_types.at(column_id);
}

bool Table::column_nullable(const ColumnID column_id) const {
  return _column_nullables.at(column_id);
}

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  Assert(chunk_id <= chunk_count(), "Chunk with ID does not exist");
  return _chunks.at(chunk_id);
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  Assert(chunk_id <= chunk_count(), "Chunk with ID does not exist");
  return _chunks.at(chunk_id);
}

void Table::compress_chunk(const ChunkID chunk_id) {
  // Implementation goes here
  Fail("Implementation is missing.");
}

}  // namespace opossum
