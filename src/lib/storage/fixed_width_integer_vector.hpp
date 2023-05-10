#pragma once

#include "abstract_attribute_vector.hpp"

namespace opossum {

// FixedWidthIntegerVector implements an attribute vector where all entries have the same size.
template <typename T>
class FixedWidthIntegerVector : public AbstractAttributeVector {
 public:
  // Returns the value id at a given position.
  ValueID get(const size_t index) const override;

  // Sets the value id at a given position.
  void set(const size_t index, const ValueID value_id) override;

  // Returns the number of values.
  size_t size() const override;

  // Returns the width of biggest value id in bytes.
  AttributeVectorWidth width() const override;

 protected:
  std::vector<T> _values;
  ValueID _biggest_value_id;
};

}  // namespace opossum
