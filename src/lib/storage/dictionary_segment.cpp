#include "dictionary_segment.hpp"

#include "utils/assert.hpp"
#include "value_segment.hpp"
#include "abstract_attribute_vector.hpp"

namespace opossum {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<AbstractSegment>& abstract_segment) {
  const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(abstract_segment);
  const auto to_be_compressed_values = std::vector<T>{value_segment->values()};
  //      typename std::vector<ColumnDataType>::iterator ip;
  //      ip = std::unique(to_be_compressed_values.begin(), to_be_compressed_values.end());
  //      // Resizing the vector so as to remove the undefined terms
  //      to_be_compressed_values.resize(std::distance(to_be_compressed_values.begin(), ip));
  auto dict = std::make_shared<std::vector<T>>(to_be_compressed_values);
  std::sort(dict->begin(), dict->end());//https://stackoverflow.com/questions/1041620/whats-the-most-efficient-way-to-erase-duplicates-and-sort-a-vector
  dict->erase(std::unique(dict->begin(), dict->end()), dict->end());
  dict->shrink_to_fit(); //

  //auto nulls = std::shared_ptr<std::vector<bool>>{}; //vector auf null values, todo: speichern von null_id in attribute-vector
  //nulls->resize(to_be_compressed_values.size());
  auto attribute_vector = std::shared_ptr<std::vector<uint32_t>>{};
  attribute_vector->resize(to_be_compressed_values.size());
  for (auto position = ColumnID{0}; position < attribute_vector->size(); position++) {
    auto id = std::distance(dict->begin(), dict->end());
    attribute_vector->at(position) = id;
  }
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  return _dictionary.at(_attribute_vector->get(chunk_offset));
}

template <typename T>
T DictionarySegment<T>::get(const ChunkOffset chunk_offset) const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
std::optional<T> DictionarySegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
const std::vector<T>& DictionarySegment<T>::dictionary() const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
std::shared_ptr<const AbstractAttributeVector> DictionarySegment<T>::attribute_vector() const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
ValueID DictionarySegment<T>::null_value_id() const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
const T DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const T value) const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const T value) const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
ChunkOffset DictionarySegment<T>::unique_values_count() const {
  // Implementation goes here
  Fail("Implementation is missing.");
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return _attribute_vector->size();
}

template <typename T>
size_t DictionarySegment<T>::estimate_memory_usage() const {
  return sizeof(T) * size();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
