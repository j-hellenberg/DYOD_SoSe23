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
  // Note that this assertion cannot fail because no chunks exist, as we create a chunk in the constructor.
  Assert(_chunks[0]->size() == 0, "It is only possible to add new columns to an empty table.");

  Assert(_column_names.size() < std::numeric_limits<ColumnCount>::max(), "Column limit is already reached.");
  resolve_data_type(type, [this, nullable](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(nullable);
    _chunks.at(0)->add_segment(value_segment);
  });
  add_column_definition(name, type, nullable);
}

void Table::create_new_chunk() {
  Assert(_chunks.size() < std::numeric_limits<ChunkID>::max(), "Chunk limit is already reached.");

  const auto chunk = std::make_shared<Chunk>();
  for (auto column_index = ColumnCount{0}; column_index < column_count(); ++column_index) {
    resolve_data_type(_column_types.at(column_index), [this, column_index, &chunk](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(_column_nullables.at(column_index));
      chunk->add_segment(value_segment);
    });
  }
  _chunks.emplace_back(chunk);
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() == target_chunk_size()) {
    create_new_chunk();
  }
  _chunks.back()->append(values);
}

ColumnCount Table::column_count() const {
  // Narrowing conversion is ok because we make sure to never have so many columns that the value overflows.
  return static_cast<ColumnCount>(_column_names.size());
}

uint64_t Table::row_count() const {
  auto row_count = uint64_t{};
  for (const auto& chunk : _chunks) {
    row_count += chunk->size();
  }
  return row_count;
}

ChunkID Table::chunk_count() const {
  // Narrowing conversion is ok because we make sure to never have so many chunks that the value overflows.
  return static_cast<ChunkID>(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto column = find(_column_names.begin(), _column_names.end(), column_name);
  Assert(column != _column_names.end(), "Column with given column name not found.");
  // Narrowing conversion is ok because we make sure to never have so many columns that the value overflows
  return static_cast<ColumnID>(std::distance(_column_names.begin(), column));
}

ChunkOffset Table::target_chunk_size() const {
  return _target_chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  return _column_names;
}

const std::string& Table::column_name(const ColumnID column_id) const {
  Assert(column_id < column_count(), "Column with ID does not exist.");
  return _column_names[column_id];
}

const std::string& Table::column_type(const ColumnID column_id) const {
  Assert(column_id < column_count(), "Column with ID does not exist.");
  return _column_types[column_id];
}

bool Table::column_nullable(const ColumnID column_id) const {
  Assert(column_id < column_count(), "Column with ID does not exist.");
  return _column_nullables[column_id];
}

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  Assert(chunk_id < chunk_count(), "Chunk with ID does not exist.");
  return _chunks[chunk_id];
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  Assert(chunk_id < chunk_count(), "Chunk with ID does not exist.");
  return _chunks[chunk_id];
}

void Table::compress_chunk(const ChunkID chunk_id) {
  // Implementation goes here
  Fail("Implementation is missing.");
}

}  // namespace opossum
