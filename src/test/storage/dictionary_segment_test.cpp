#include "base_test.hpp"

#include "resolve_type.hpp"
#include "storage/abstract_attribute_vector.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/dictionary_segment.hpp"

namespace opossum {

class StorageDictionarySegmentTest : public BaseTest {
 protected:
  std::shared_ptr<ValueSegment<int32_t>> value_segment_int{std::make_shared<ValueSegment<int32_t>>()};
  std::shared_ptr<ValueSegment<std::string>> value_segment_str{std::make_shared<ValueSegment<std::string>>(true)};
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  value_segment_str->append("Bill");
  value_segment_str->append("Steve");
  value_segment_str->append("Alexander");
  value_segment_str->append("Steve");
  value_segment_str->append("Hasso");
  value_segment_str->append("Bill");
  value_segment_str->append(NULL_VALUE);

  const auto dict_segment = std::make_shared<DictionarySegment<std::string>>(value_segment_str);

  // Test conversion of ValueSegment to DictionarySegment
  EXPECT_THROW(std::make_shared<DictionarySegment<int32_t>>(value_segment_str), std::logic_error);

  // Test attribute_vector size.
  EXPECT_EQ(dict_segment->size(), 7);

  // Test dictionary size (uniqueness).
  EXPECT_EQ(dict_segment->unique_values_count(), 4);

  // Test value of value id
  EXPECT_EQ(dict_segment->value_of_value_id(ValueID{1}), "Bill");

  // Test sorting.
  const auto& dict = dict_segment->dictionary();
  EXPECT_EQ(dict[0], "Alexander");
  EXPECT_EQ(dict[1], "Bill");
  EXPECT_EQ(dict[2], "Hasso");
  EXPECT_EQ(dict[3], "Steve");
  EXPECT_EQ(dict.capacity(), 4);

  // Test accessing dict segment
  EXPECT_EQ(dict_segment->get(0), "Bill");
  EXPECT_EQ(dict_segment->get_typed_value(0), "Bill");
  EXPECT_EQ((*dict_segment)[0], AllTypeVariant{"Bill"});

  // Test NULL value handling.
  EXPECT_EQ(dict_segment->attribute_vector()->get(6), dict_segment->null_value_id());
  EXPECT_EQ(dict_segment->get_typed_value(6), std::nullopt);
  EXPECT_THROW(dict_segment->get(6), std::logic_error);
}

TEST_F(StorageDictionarySegmentTest, DifferentTypes) {
  std::shared_ptr<ValueSegment<float>> value_segment_float{std::make_shared<ValueSegment<float>>()};
  std::shared_ptr<ValueSegment<double>> value_segment_double{std::make_shared<ValueSegment<double>>()};

  value_segment_double->append(2.3);
  value_segment_double->append(1.2);
  value_segment_double->append(2.3);

  const auto dict_double = std::make_shared<DictionarySegment<double>>(value_segment_double);
  EXPECT_EQ(dict_double->get(0), 2.3);
  EXPECT_EQ(dict_double->size(), 3);
  EXPECT_EQ(dict_double->unique_values_count(), 2);
  EXPECT_EQ(dict_double->estimate_memory_usage(), 2 * sizeof(double) + 3 * sizeof(uint8_t));

  value_segment_float->append(3.4f);
  value_segment_float->append(5.2f);
  value_segment_float->append(0.0f);
  value_segment_float->append(5.2f);

  const auto dict_float = std::make_shared<DictionarySegment<float>>(value_segment_float);
  EXPECT_EQ(dict_float->get(2), 0.0f);
  EXPECT_EQ(dict_float->size(), 4);
  EXPECT_EQ(dict_float->unique_values_count(), 3);
  EXPECT_EQ(dict_float->estimate_memory_usage(), 3 * sizeof(float) + 4 * sizeof(uint8_t));
}

TEST_F(StorageDictionarySegmentTest, DefaultValueIsNotConvertedToNullValue) {
  value_segment_str->append("");

  const auto dict_segment = std::make_shared<DictionarySegment<std::string>>(value_segment_str);
  EXPECT_EQ(dict_segment->get(0), "");
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (auto value = int16_t{0}; value <= 10; value += 2) {
    value_segment_int->append(value);
  }

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->lower_bound(4), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(4), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant{4}), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant{4}), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(5), ValueID{3});
  EXPECT_EQ(dict_segment->upper_bound(5), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(15), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment->upper_bound(15), INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, DifferentNumberOfDistinctValues) {
  auto dict_segment = std::make_shared<DictionarySegment<int>>(value_segment_int);
  EXPECT_EQ(dict_segment->attribute_vector()->width(), sizeof(uint8_t));
  EXPECT_EQ(dict_segment->null_value_id(), std::numeric_limits<uint8_t>::max());

  for (auto value = int32_t{0}; value < std::numeric_limits<uint8_t>::max() + 1; ++value) {
    value_segment_int->append(value);
  }
  dict_segment = std::make_shared<DictionarySegment<int>>(value_segment_int);
  EXPECT_EQ(dict_segment->attribute_vector()->width(), sizeof(uint16_t));
  EXPECT_EQ(dict_segment->null_value_id(), std::numeric_limits<uint16_t>::max());
}

TEST_F(StorageDictionarySegmentTest, EstimateMemoryConsumption) {
  value_segment_int->append(0);
  value_segment_int->append(1);
  value_segment_int->append(2);
  auto dict_segment = std::make_shared<DictionarySegment<int32_t>>(value_segment_int);
  // The dictionary contains 3 entries, which are all distinct and need one dictionary place each
  EXPECT_EQ(dict_segment->estimate_memory_usage(), sizeof(uint8_t) * 3 + sizeof(int32_t) * 3);

  value_segment_int->append(1);
  value_segment_int->append(1);
  value_segment_int->append(1);
  dict_segment = std::make_shared<DictionarySegment<int32_t>>(value_segment_int);
  // The number of elements in the attribute_vector has increased compared to before,
  // but the number of dictionary entries has not
  EXPECT_EQ(dict_segment->estimate_memory_usage(), sizeof(uint8_t) * 6 + sizeof(int32_t) * 3);

  value_segment_int->append(3);
  dict_segment = std::make_shared<DictionarySegment<int32_t>>(value_segment_int);
  // We appended another value that was not yet contained in the segment; therefore, both the attribute vector and
  // the dictionary increased in size.
  EXPECT_EQ(dict_segment->estimate_memory_usage(), sizeof(uint8_t) * 7 + sizeof(int32_t) * 4);
}

}  // namespace opossum
