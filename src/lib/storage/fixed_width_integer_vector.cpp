#include "fixed_width_integer_vector.hpp"
#include "all_type_variant.hpp"

#include <bit>

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

    if (value_id > _biggest_value_id && value_id != INVALID_VALUE_ID) {
      _biggest_value_id = value_id;
    }
  }

  template <typename T>
  size_t FixedWidthIntegerVector<T>::size() const {
    return _values.size();
  }

  template <typename T>
  AttributeVectorWidth FixedWidthIntegerVector<T>::width() const {
    return (8 * sizeof(T)) - std::countl_zero(static_cast<T>(_biggest_value_id));
  }

  template class FixedWidthIntegerVector<uint32_t>;
  template class FixedWidthIntegerVector<uint16_t>;
  template class FixedWidthIntegerVector<uint8_t>;
} // namespace opossum
