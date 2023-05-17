#include "chunk.hpp"
#include <boost/hana/for_each.hpp>

#include "abstract_segment.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

void Chunk::add_segment(const std::shared_ptr<AbstractSegment> segment) {
  Assert(_segments.size() < std::numeric_limits<ColumnCount>::max(), "Segment limit is already reached.");
  _segments.emplace_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  Assert(values.size() == _segments.size(), "Number of values and number of columns should be equal.");

  for (auto segment_index = size_t{0}; segment_index < _segments.size(); ++segment_index) {
    const auto& value = values[segment_index];
    const auto segment = _segments[segment_index];

    // If our value is not a NullValue, we could infer from value.type() which type of ValueSegment we need
    // to cast our segment to in order to be able to append values to it.
    // However, if the value is NullValue, we have no way to infer this. Therefore, we have no choice but to
    // just try everything until we find something that works...
    auto append_successful = false;
    hana::for_each(opossum::types, [&](auto opossum_type) {
      using Type = typename decltype(opossum_type)::type;
      if (const auto& value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
          !append_successful && value_segment) {
        value_segment->append(value);
        append_successful = true;
      }
    });
    Assert(append_successful,
           "Either some segment of the chunk is not a ValueSegment or a value of unknown type was given.");
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const {
  return _segments.at(column_id);
}

ColumnCount Chunk::column_count() const {
  // Narrowing conversion is ok because we make sure to never have as many columns that the value overflows.
  return static_cast<ColumnCount>(_segments.size());
}

ChunkOffset Chunk::size() const {
  if (_segments.empty()) {
    return ChunkOffset{0};
  }
  return ChunkOffset{_segments.at(0)->size()};
}

}  // namespace opossum
