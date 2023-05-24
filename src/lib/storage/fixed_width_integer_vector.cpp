#include "fixed_width_integer_vector.hpp"
#include "dictionary_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {
template <typename uintX_t>
FixedWidthIntegerVector<uintX_t>::FixedWidthIntegerVector(const std::vector<ValueID>& values) {
  _values.reserve(values.size());
  for (const auto value : values) {
    Assert(value == NULL_VALUE_ID || value <= std::numeric_limits<uintX_t>::max(),
           "Passed value " + std::to_string(value) + " is too big to fit into uint*_t data type of our vector.");
    _values.push_back(static_cast<uintX_t>(value));
  }
}

template <typename uintX_t>
ValueID FixedWidthIntegerVector<uintX_t>::get(const size_t index) const {
  Assert(index < _values.size(), "Invalid index given.");
  return static_cast<ValueID>(_values[index]);
}

template <typename uintX_t>
void FixedWidthIntegerVector<uintX_t>::set(const size_t index, const ValueID value_id) {
  Assert(index < _values.size(), "Index out of bounds for vector and size of vector is fixed (may not be increased).");
  Assert(value_id == NULL_VALUE_ID || value_id <= std::numeric_limits<uintX_t>::max(),
         "Passed value " + std::to_string(value_id) + " is too big to fit into uint*_t data type of our vector.");
  _values[index] = static_cast<uintX_t>(value_id);
}

template <typename uintX_t>
size_t FixedWidthIntegerVector<uintX_t>::size() const {
  return _values.size();
}

template <typename uintX_t>
AttributeVectorWidth FixedWidthIntegerVector<uintX_t>::width() const {
  return sizeof(uintX_t);
}

template class FixedWidthIntegerVector<uint32_t>;
template class FixedWidthIntegerVector<uint16_t>;
template class FixedWidthIntegerVector<uint8_t>;
}  // namespace opossum
