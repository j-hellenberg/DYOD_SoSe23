#pragma once

#include "abstract_attribute_vector.hpp"

namespace opossum {

// FixedWidthIntegerVector implements an attribute vector where all entries have the same size.
template <typename uintX_t>
class FixedWidthIntegerVector : public AbstractAttributeVector {
 public:
  // Create the vector directly from a normal std::vector so that we avoid having to resize it on every
  // new value we set.
  explicit FixedWidthIntegerVector(const std::vector<ValueID>& values);

  // Returns the value id at a given position.
  ValueID get(const size_t index) const override;

  // Sets the value id at a given position.
  void set(const size_t index, const ValueID value_id) override;

  // Returns the number of values.
  size_t size() const override;

  // Returns the width of biggest value id in bytes.
  AttributeVectorWidth width() const override;

 protected:
  std::vector<uintX_t> _values;
};

}  // namespace opossum
