#include "dictionary_segment.hpp"

#include <algorithm>
#include "abstract_attribute_vector.hpp"
#include "fixed_width_integer_vector.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<AbstractSegment>& abstract_segment) {
  const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(abstract_segment);
  DebugAssert(value_segment, "Can only construct a DictionarySegment from a value segment of matching type.");
  construct_dictionary(value_segment);
  construct_attribute_vector(value_segment);
}

template <typename T>
void DictionarySegment<T>::construct_dictionary(const std::shared_ptr<ValueSegment<T>>& value_segment) {
  const auto original_values = value_segment->values();
  _dictionary.reserve(original_values.size());
  // We need to make sure to only put values in our dictionary that don't correspond to NULL_VALUE.
  // However, we cannot just remove the default value for T (for example, "" for std::string) from our final dictionary,
  // because somebody might have actually inserted this value without meaning the NULL_VALUE.
  for (auto offset = ChunkOffset{0}; offset < original_values.size(); ++offset) {
    if (!value_segment->is_null(offset)) {
      _dictionary.push_back(original_values[offset]);
    }
  }

  std::sort(_dictionary.begin(), _dictionary.end());
  _dictionary.erase(std::unique(_dictionary.begin(), _dictionary.end()), _dictionary.end());
  _dictionary.shrink_to_fit();
}

template <typename T>
void DictionarySegment<T>::construct_attribute_vector(const std::shared_ptr<ValueSegment<T>>& value_segment) {
  // Selecting the appropriate datatype for our attribute vector depending on how many distinct values we have.
  if (_dictionary.size() > std::numeric_limits<uint16_t>::max()) {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint32_t>>();
    _null_value_id = INVALID_VALUE_ID;
  } else if (_dictionary.size() > std::numeric_limits<uint8_t>::max()) {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>();
    _null_value_id = static_cast<uint16_t>(INVALID_VALUE_ID);
  } else {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint8_t>>();
    _null_value_id = static_cast<uint8_t>(INVALID_VALUE_ID);
  }

  const auto original_values = value_segment->values();
  for (auto offset = ChunkOffset{0}; offset < original_values.size(); ++offset) {
    if (value_segment->is_null(offset)) {
      _attribute_vector->set(offset, INVALID_VALUE_ID);
    } else {
      _attribute_vector->set(offset, value_id_for_value(original_values[offset]));
    }
  }
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  return _dictionary.at(_attribute_vector->get(chunk_offset));
}

template <typename T>
T DictionarySegment<T>::get(const ChunkOffset chunk_offset) const {
  Assert(_attribute_vector->get(chunk_offset) != null_value_id(), "Trying to access data that points to a NULL_VALUE.");
  return _dictionary.at(_attribute_vector->get(chunk_offset));
}

template <typename T>
std::optional<T> DictionarySegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  if (_attribute_vector->get(chunk_offset) == null_value_id()) {
    return std::nullopt;
  }
  return get(chunk_offset);
}

template <typename T>
const std::vector<T>& DictionarySegment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<const AbstractAttributeVector> DictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
ValueID DictionarySegment<T>::null_value_id() const {
  return _null_value_id;
}

template <typename T>
const T DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  return _dictionary.at(value_id);
}

template <typename T>
ValueID DictionarySegment<T>::value_id_for_value(const T value) const {
  const auto index = find(_dictionary.begin(), _dictionary.end(), value);
  DebugAssert(index != _dictionary.end(), "Value is not contained in dictionary.");
  return static_cast<ValueID>(std::distance(_dictionary.begin(), index));
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const T value) const {
  const auto lower_bound_position = std::lower_bound(_dictionary.begin(), _dictionary.end(), value);
  if (lower_bound_position == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }
  return static_cast<ValueID>(std::distance(_dictionary.begin(), lower_bound_position));
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  return lower_bound(type_cast<T>(value));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const T value) const {
  const auto upper_bound_position = std::upper_bound(_dictionary.begin(), _dictionary.end(), value);
  if (upper_bound_position == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }
  return static_cast<ValueID>(std::distance(_dictionary.begin(), upper_bound_position));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  return upper_bound(type_cast<T>(value));
}

template <typename T>
ChunkOffset DictionarySegment<T>::unique_values_count() const {
  return _dictionary.size();
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return _attribute_vector->size();
}

template <typename T>
size_t DictionarySegment<T>::estimate_memory_usage() const {
  return _attribute_vector->width() * size() + sizeof(T) * _dictionary.size();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
