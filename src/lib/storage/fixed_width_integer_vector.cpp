#include "fixed_width_integer_vector.hpp"
#include "all_type_variant.hpp"

namespace opossum {
template <typename T>
ValueID FixedWidthIntegerVector<T>::get(const size_t index) const {
  return static_cast<ValueID>(_values.at(index));
}

template <typename T>
void FixedWidthIntegerVector<T>::set(const size_t index, const ValueID value_id) {
  if (_values.size() <= index) {
    _values.resize(index + 1);
  }
  _values[index] = value_id;
}

template <typename T>
size_t FixedWidthIntegerVector<T>::size() const {
  return _values.size();
}

template <typename T>
AttributeVectorWidth FixedWidthIntegerVector<T>::width() const {
  return sizeof(T);
}

template class FixedWidthIntegerVector<uint32_t>;
template class FixedWidthIntegerVector<uint16_t>;
template class FixedWidthIntegerVector<uint8_t>;
}  // namespace opossum
