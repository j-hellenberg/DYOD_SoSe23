#include "table.hpp"

#include <mutex>
#include <thread>
#include "dictionary_segment.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {
Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size(target_chunk_size) {
  create_new_chunk();
}

Table::Table(std::shared_ptr<const Table>& old_table_for_metadata, std::vector<std::shared_ptr<Chunk>>& chunks)
    : Table() {
  _copy_metadata_from(old_table_for_metadata);

  if (!chunks.empty()) {
    _chunks.clear();
    _chunks.reserve(chunks.size());
    for (auto& chunk : chunks) {
      _chunks.push_back(chunk);
    }
  }
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

void Table::_compress_segment_and_add_to_chunk(ColumnID index,
                                               std::vector<std::shared_ptr<AbstractSegment>>& compressed_segments,
                                               const std::shared_ptr<Chunk>& chunk_to_be_compressed) const {
  const auto segment = chunk_to_be_compressed->get_segment(index);
  resolve_data_type(column_type(index), [&index, &compressed_segments, &segment](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    compressed_segments[index] = std::make_shared<DictionarySegment<ColumnDataType>>(segment);
  });
}

void Table::compress_chunk(const ChunkID chunk_id) {
  Assert(chunk_id < chunk_count(), "Chunk with ID does not exist");
  if (chunk_id == chunk_count() - 1) {
    create_new_chunk();
  }

  const auto chunk_to_be_compressed = get_chunk(chunk_id);
  const auto segment_count = column_count();
  auto compression_threads = std::vector<std::thread>{};
  compression_threads.reserve(segment_count);
  auto compressed_segments = std::vector<std::shared_ptr<AbstractSegment>>{};
  compressed_segments.resize(segment_count);
  for (auto index = ColumnID{0}; index < segment_count; index++) {
    compression_threads.emplace_back(&Table::_compress_segment_and_add_to_chunk, this, index,
                                     std::ref(compressed_segments), std::cref(chunk_to_be_compressed));
  }
  for (auto& thread : compression_threads) {
    thread.join();
  }

  // Collect the segments we just compressed and insert them into a new chunk.
  // Note that we needed to insert them in the compressed_segments vector first because we could not be sure in which
  // order the threads used for compression would finish.
  const auto new_chunk = std::make_shared<Chunk>();
  for (const auto& segment : compressed_segments) {
    new_chunk->add_segment(segment);
  }
  // Swap out the old chunk with the compressed chunk. The old chunk will stay valid until no-one is referencing it
  // anymore (which is fine because both contain the same data).
  // Note that this will not lead to any data races regarding row insertion because, if we are told to compress
  // the last chunk, we have created a new one before starting the compression. This new chunk will then receive the
  // insertions.
  // Somebody may still manually add rows to the chunk we are compressing, which we would miss during the compression,
  // but since doing that is violates patterns of intended usage, we don't address this edge case here.
  // (It would also be basically impossible to prevent that here by, e.g., locking access to the chunk,
  // as somebody may already have a reference to that chunk).
  _chunks[chunk_id] = new_chunk;
}

void Table::_copy_metadata_from(std::shared_ptr<const Table>& other_table) {
  auto column_count = other_table->column_count();
  for (auto column_index = ColumnID{0}; column_index < column_count; ++column_index) {
    add_column(other_table->column_name(column_index), other_table->column_type(column_index),
               other_table->column_nullable(column_index));
  }
}
}  // namespace opossum
