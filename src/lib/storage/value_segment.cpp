#include "value_segment.hpp"

#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
ValueSegment<T>::ValueSegment(bool nullable) : _is_nullable(nullable) {}

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  if (is_null(chunk_offset)) {
    return NULL_VALUE;
  }

  return _values.at(chunk_offset);
}

template <typename T>
bool ValueSegment<T>::is_null(const ChunkOffset chunk_offset) const {
  if (!is_nullable()) {
    return false;
  }

  return _nulls.at(chunk_offset);
}

template <typename T>
T ValueSegment<T>::get(const ChunkOffset chunk_offset) const {
  if (is_null(chunk_offset)) {
    Fail("No value present at offset");
  }

  return _values.at(chunk_offset);
}

template <typename T>
std::optional<T> ValueSegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  if (is_null(chunk_offset)) {
    return std::nullopt;
  }

  return std::make_optional<T>(get(chunk_offset));
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& value) {
  if (variant_is_null(value)) {
    Assert(is_nullable(), "Trying to append NullValue to not nullable Segment.");
    _nulls.emplace_back(true);
    _values.emplace_back(T{});
  } else {
    try {
      _nulls.emplace_back(false);
      _values.emplace_back(type_cast<T>(value));
    } catch (boost::wrapexcept<boost::bad_lexical_cast>& e) {
      Fail("Cannot convert given value to type stored in segment.");
    }
  }
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return _values.size();
}

template <typename T>
const std::vector<T>& ValueSegment<T>::values() const {
  return _values;
}

template <typename T>
bool ValueSegment<T>::is_nullable() const {
  return _is_nullable;
}

template <typename T>
const std::vector<bool>& ValueSegment<T>::null_values() const {
  Assert(is_nullable(), "Can only get null_values for segment supporting them.");
  return _nulls;
}

template <typename T>
size_t ValueSegment<T>::estimate_memory_usage() const {
  return sizeof(T) * size();
}

// Macro to instantiate the following classes:
// template class ValueSegment<int32_t>;
// template class ValueSegment<int64_t>;
// template class ValueSegment<float>;
// template class ValueSegment<double>;
// template class ValueSegment<std::string>;
EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
