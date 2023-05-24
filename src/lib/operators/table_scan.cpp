#include "table_scan.hpp"
#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "storage/value_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "type_cast.hpp"
#include "storage/abstract_attribute_vector.hpp"

namespace opossum {
TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value) : AbstractOperator(in, nullptr),
_column_id(column_id), _scan_type(scan_type), _search_value(search_value) {
}

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
      if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(input_chunk->get_segment(column_index)); reference_segment) {
        output_chunk->add_segment(std::make_shared<ReferenceSegment>(
            reference_segment->referenced_table(), reference_segment->referenced_column_id(), pos_list
            ));
      } else {
        output_chunk->add_segment(std::make_shared<ReferenceSegment>(
            input_table, column_index, pos_list
            ));
      }
    }
    output_chunks.push_back(output_chunk);
  }

  auto output_table = std::make_shared<Table>(input_table, output_chunks);
  return output_table;
}

std::shared_ptr<const PosList> TableScan::_filter(std::string& column_type, std::shared_ptr<const Chunk> chunk, ChunkID& chunk_id) const {
  auto target_segment = chunk->get_segment(_column_id);
  auto filtered_position = std::make_shared<PosList>();
  resolve_data_type(column_type, [this, &target_segment, &chunk_id, &filtered_position] (auto type) {
    using ColumnType = typename decltype(type)::type;
    // TODO: make this assert work
    // Assert(typeid(ColumnType) == _search_value.type(), "Search value doesn't have the same type as the column, we want to compare it with.");
    auto _typed_search_value = type_cast<ColumnType>(_search_value);

//    if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(target_segment); reference_segment) {
//      auto segment_size = reference_segment->size();
//      for (auto position_list_index = size_t{0}; position_list_index < segment_size; ++position_list_index) {
//        auto row = reference_segment->pos_list()->operator[](position_list_index);
//        auto actual_target_segment = reference_segment->referenced_table()->get_chunk(row.chunk_id)->get_segment(reference_segment->referenced_column_id());
//
//      }
//    } else {
//
//    }

    if (auto value_segment = std::dynamic_pointer_cast<ValueSegment<ColumnType>>(target_segment); value_segment) {
      std::function<bool(ColumnType)> comparison_func;
      switch (_scan_type) {
        case ScanType::OpEquals:
          comparison_func = [_typed_search_value](auto row_value) { return row_value == _typed_search_value; };
          break;
        case ScanType::OpNotEquals:
          comparison_func = [_typed_search_value](auto row_value) { return row_value != _typed_search_value; };
          break;
        case ScanType::OpLessThan:
          comparison_func = [_typed_search_value](auto row_value) { return row_value < _typed_search_value; };
          break;
        case ScanType::OpLessThanEquals:
          comparison_func = [_typed_search_value](auto row_value) { return row_value <= _typed_search_value; };
          break;
        case ScanType::OpGreaterThan:
          comparison_func = [_typed_search_value](auto row_value) { return row_value > _typed_search_value; };
          break;
        case ScanType::OpGreaterThanEquals:
          comparison_func = [_typed_search_value](auto row_value) { return row_value >= _typed_search_value; };
          break;
      }

      for (auto row_index = ChunkOffset{0}; row_index < value_segment->size(); ++row_index) {
        if (comparison_func(value_segment->get(row_index))) {
          filtered_position->push_back(RowID{chunk_id, row_index});
        }
      }
    } else if (auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnType>>(target_segment); dictionary_segment) {
      std::function<bool(ValueID)> comparison_func;
      auto search_value_id_low = dictionary_segment->lower_bound(_search_value);
      auto search_value_id_upp = dictionary_segment->upper_bound(_search_value);
      switch (_scan_type) {
        case ScanType::OpEquals:
          comparison_func = [search_value_id_low](auto row_value_id) { return row_value_id == search_value_id_low; };
          break;
        case ScanType::OpNotEquals:
          comparison_func = [search_value_id_low](auto row_value_id) { return row_value_id != search_value_id_low; };
          break;
        case ScanType::OpLessThan:
          comparison_func = [search_value_id_upp](auto row_value_id) { return row_value_id < search_value_id_upp; };
          break;
        case ScanType::OpLessThanEquals:
          comparison_func = [search_value_id_upp](auto row_value_id) { return row_value_id <= search_value_id_upp; };
          break;
        case ScanType::OpGreaterThan:
          comparison_func = [search_value_id_low](auto row_value_id) { return row_value_id > search_value_id_low; };
          break;
        case ScanType::OpGreaterThanEquals:
          comparison_func = [search_value_id_low](auto row_value_id) { return row_value_id >= search_value_id_low; };
          break;
      }

      for (auto row_index = ChunkOffset{0}; row_index < dictionary_segment->size(); ++row_index) {
        if (comparison_func(dictionary_segment->attribute_vector()->get(row_index))) {
          filtered_position->push_back(RowID{chunk_id, row_index});
        }
      }
    } else {
      Fail("Unknown segment type encountered.");
    }
  });
  return filtered_position;
}
}  // namespace opossum
