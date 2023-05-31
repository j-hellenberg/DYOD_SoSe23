#include "table_scan.hpp"
#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_attribute_vector.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {
TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator(in, nullptr), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const {
  return _column_id;
}

ScanType TableScan::scan_type() const {
  return _scan_type;
}

const AllTypeVariant& TableScan::search_value() const {
  return _search_value;
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  auto input_table = _left_input_table();

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
  output_chunks.reserve(input_table->chunk_count());
  for (auto chunk_index = ChunkID{0}; chunk_index < input_table->chunk_count(); ++chunk_index) {
    auto filter_column_type = input_table->column_type(_column_id);
    auto input_chunk = input_table->get_chunk(chunk_index);
    auto pos_list = _filter(filter_column_type, input_chunk, chunk_index);
    if (pos_list->empty()) {
      continue;
    }

    auto output_chunk = std::make_shared<Chunk>();
    for (auto column_index = ColumnID{0}; column_index < input_table->column_count(); ++column_index) {
      if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(input_chunk->get_segment(column_index));
          reference_segment) {
        output_chunk->add_segment(std::make_shared<ReferenceSegment>(
            reference_segment->referenced_table(), reference_segment->referenced_column_id(), pos_list));
      } else {
        output_chunk->add_segment(std::make_shared<ReferenceSegment>(input_table, column_index, pos_list));
      }
    }
    output_chunks.push_back(output_chunk);
  }

  return std::make_shared<Table>(input_table, output_chunks);
}

std::shared_ptr<const PosList> TableScan::_filter(std::string& column_type, std::shared_ptr<const Chunk> chunk,
                                                  ChunkID& chunk_id) {
  auto target_segment = chunk->get_segment(_column_id);
  auto filtered_position = std::make_shared<PosList>();
  auto filter_func = _filter_function_for_segment(column_type, target_segment);
  for (auto row_index = ChunkOffset{0}; row_index < target_segment->size(); ++row_index) {
    if (filter_func(row_index)) {
      filtered_position->push_back(RowID{chunk_id, row_index});
    }
  }
  return filtered_position;
}

std::function<bool(ChunkOffset)> TableScan::_filter_function_for_segment(
    std::string& column_type, std::shared_ptr<AbstractSegment>& target_segment) {
  if (!_filter_functions.contains(target_segment.get())) {
    resolve_data_type(column_type, [this, &column_type, &target_segment](auto type) {
      using ColumnType = typename decltype(type)::type;
      Assert(typeid(ColumnType) == _search_value.type(),
             "Search value doesn't have the same type as the column, we want to compare it with.");
      auto _typed_search_value = type_cast<ColumnType>(_search_value);

      std::function<bool(ChunkOffset)> filter_function;
      if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(target_segment); reference_segment) {
        filter_function = [this, &column_type, reference_segment](auto row_index) {
          auto row = reference_segment->pos_list()->operator[](row_index);
          auto actual_target_segment = reference_segment->referenced_table()
                                           ->get_chunk(row.chunk_id)
                                           ->get_segment(reference_segment->referenced_column_id());
          return this->_filter_function_for_segment(column_type, actual_target_segment)(row_index);
        };
      } else if (auto value_segment = std::dynamic_pointer_cast<ValueSegment<ColumnType>>(target_segment);
                 value_segment) {
        std::function<bool(ColumnType, ColumnType)> comparator;
        switch (_scan_type) {
          case ScanType::OpEquals:
            comparator = [](auto value, auto search_value) { return value == search_value; };
            break;
          case ScanType::OpNotEquals:
            comparator = [](auto value, auto search_value) { return value != search_value; };
            break;
          case ScanType::OpLessThan:
            comparator = [](auto value, auto search_value) { return value < search_value; };
            break;
          case ScanType::OpLessThanEquals:
            comparator = [](auto value, auto search_value) { return value <= search_value; };
            break;
          case ScanType::OpGreaterThan:
            comparator = [](auto value, auto search_value) { return value > search_value; };
            break;
          case ScanType::OpGreaterThanEquals:
            comparator = [](auto value, auto search_value) { return value >= search_value; };
            break;
        }
        filter_function = [_typed_search_value, value_segment, comparator](auto row_index) {
          return !value_segment->is_null(row_index) && comparator(value_segment->get(row_index), _typed_search_value);
        };
      } else if (auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnType>>(target_segment);
                 dictionary_segment) {
        auto search_value_id_low = dictionary_segment->lower_bound(_search_value);
        auto search_value_id_upp = dictionary_segment->upper_bound(_search_value);
        std::function<bool(ValueID)> comparator;
        switch (_scan_type) {
          case ScanType::OpEquals:
            if (search_value_id_low != search_value_id_upp) {
              comparator = [search_value_id_low](auto search_value_id) {
                return search_value_id == search_value_id_low;
              };
            } else {
              comparator = [](auto search_value_id) { return false; };
            }
            break;
          case ScanType::OpNotEquals:
            if (search_value_id_low != search_value_id_upp) {
              comparator = [search_value_id_low](auto search_value_id) {
                return search_value_id != search_value_id_low;
              };
            } else {
              comparator = [](auto search_value_id) { return true; };
            }
            break;
          case ScanType::OpLessThan:
            comparator = [search_value_id_low](auto search_value_id) { return search_value_id < search_value_id_low; };
            break;
          case ScanType::OpLessThanEquals:
            if (search_value_id_low != search_value_id_upp) {
              comparator = [search_value_id_low](auto search_value_id) {
                return search_value_id <= search_value_id_low;
              };
            } else {
              comparator = [search_value_id_low](auto search_value_id) {
                return search_value_id < search_value_id_low;
              };
            }
            break;
          case ScanType::OpGreaterThan:
            comparator = [search_value_id_upp](auto search_value_id) { return search_value_id >= search_value_id_upp; };
            break;
          case ScanType::OpGreaterThanEquals:
            comparator = [search_value_id_low](auto search_value_id) { return search_value_id >= search_value_id_low; };
            break;
        }
        filter_function = [dictionary_segment, comparator](auto row_index) {
          return comparator(dictionary_segment->attribute_vector()->get(row_index));
        };
      } else {
        Fail("Unknown segment type encountered.");
      }
      _filter_functions.insert({target_segment.get(), filter_function});
    });
  }

  return _filter_functions.at(target_segment.get());
}
}  // namespace opossum
