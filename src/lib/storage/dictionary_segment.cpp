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
  Assert(value_segment, "Can only construct a DictionarySegment from a value segment of matching type.");
  _construct_dictionary(value_segment);
  _construct_attribute_vector(value_segment);
}

template <typename T>
void DictionarySegment<T>::_construct_dictionary(const std::shared_ptr<ValueSegment<T>>& value_segment) {
  const auto original_values = value_segment->values();
  const auto num_values = original_values.size();
  _dictionary.reserve(original_values.size());
  // We need to make sure to only put values in our dictionary that don't correspond to NULL_VALUE.
  // However, we cannot just remove the default value for T (for example, "" for std::string) from our final dictionary,
  // because somebody might have actually inserted this value without meaning the NULL_VALUE.
  for (auto offset = ChunkOffset{0}; offset < num_values; ++offset) {
    if (!value_segment->is_null(offset)) {
      _dictionary.push_back(original_values[offset]);
    }
  }

  std::sort(_dictionary.begin(), _dictionary.end());
  _dictionary.erase(std::unique(_dictionary.begin(), _dictionary.end()), _dictionary.end());
  _dictionary.shrink_to_fit();
}

template <typename T>
void DictionarySegment<T>::_construct_attribute_vector(const std::shared_ptr<ValueSegment<T>>& value_segment) {
  const auto original_values = value_segment->values();
  const auto num_values = original_values.size();

  auto value_ids_for_values = std::vector<ValueID>{};
  value_ids_for_values.reserve(num_values);
  for (auto offset = ChunkOffset{0}; offset < num_values; ++offset) {
    if (value_segment->is_null(offset)) {
      value_ids_for_values.push_back(NULL_VALUE_ID);
    } else {
      value_ids_for_values.push_back(_value_id_for_value(original_values[offset]));
    }
  }

  // Selecting the appropriate datatype for our attribute vector depending on how many distinct values we have.
  // We need to use size() + 1 here because we need to be able to distinguish any valid ValueID from our
  // _null_value_id.
  if (_dictionary.size() + 1 > std::numeric_limits<uint16_t>::max()) {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint32_t>>(value_ids_for_values);
    _null_value_id = NULL_VALUE_ID;
  } else if (_dictionary.size() + 1 > std::numeric_limits<uint8_t>::max()) {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(value_ids_for_values);
    _null_value_id = static_cast<uint16_t>(NULL_VALUE_ID);
  } else {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint8_t>>(value_ids_for_values);
    _null_value_id = static_cast<uint8_t>(NULL_VALUE_ID);
  }
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  const auto optional_value = get_typed_value(chunk_offset);
  if (optional_value) {
    return optional_value.value();
  }
  return NULL_VALUE;
}

template <typename T>
T DictionarySegment<T>::get(const ChunkOffset chunk_offset) const {
  const auto optional_value = get_typed_value(chunk_offset);
  Assert(optional_value, "Trying to access data that points to a NULL_VALUE.");
  return optional_value.value();
}

template <typename T>
std::optional<T> DictionarySegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  const auto value_id_for_offset = _attribute_vector->get(chunk_offset);
  if (value_id_for_offset == null_value_id()) {
    return std::nullopt;
  }
  return value_of_value_id(value_id_for_offset);
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
  Assert(value_id < _dictionary.size(), "Given value ID is not contained in the dictionary.");
  return _dictionary[value_id];
}

template <typename T>
ValueID DictionarySegment<T>::_value_id_for_value(const T value) const {
  const auto index = find(_dictionary.begin(), _dictionary.end(), value);
  // Note that by returning, e.g., INVALID_VALUE_ID, we could use this method to check whether the value is
  // actually contained in the dictionary.
  // However, this method is only used internally during construction of the segment.
  // Therefore, every value we query here should be contained in the dictionary.
  // In order to avoid having to check the return value of this method, we decided to use an assertion here.
  Assert(index != _dictionary.end(), "Value is not contained in dictionary.");
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
  return _attribute_vector->width() * _attribute_vector->size() + sizeof(T) * _dictionary.capacity();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
