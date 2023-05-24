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
  auto output_table = Table();
  auto input_table = _left_input_table();
  input_table->copy_metadata_to(output_table);
  for (auto chunk_index = ChunkID{0}; chunk_index < input_table->chunk_count(); ++chunk_index) {
    auto filter_column_type = input_table->column_type(_column_id);
    auto pos_list = _filter(filter_column_type, input_table->get_chunk(chunk_index), chunk_index);
  }
  return std::shared_ptr<const Table>(&output_table);
}

std::shared_ptr<const PosList> TableScan::_filter(std::string& column_type, std::shared_ptr<const Chunk> chunk, ChunkID& chunk_id) const {
  auto target_segment = chunk->get_segment(_column_id);
  auto filtered_position = PosList();
  resolve_data_type(column_type, [this, &target_segment, &chunk_id, &filtered_position] (auto type) {
    using ColumnType = typename decltype(type)::type;
    Assert(typeid(ColumnType) == _search_value.type(), "Search value doesn't have the same type as the column, we want to compare it with.");
    if (auto value_segment = std::dynamic_pointer_cast<ValueSegment<ColumnType>>(target_segment); value_segment) {
      for (auto value_index = ChunkOffset{0}; value_index <= value_segment->values().size(); ++value_index) {
        if (_scan_type == ScanType::OpEquals) {
          if (value_segment->get(value_index) == type_cast<ColumnType>(_search_value)) {
            filtered_position.push_back(RowID{chunk_id, value_index});
          }
        } else if (_scan_type == ScanType::OpNotEquals) {
          if (value_segment->get(value_index) != type_cast<ColumnType>(_search_value)) {
            filtered_position.push_back(RowID{chunk_id, value_index});
          }
        } else if (_scan_type == ScanType::OpLessThan) {
          if (value_segment->get(value_index) < type_cast<ColumnType>(_search_value)) {
            filtered_position.push_back(RowID{chunk_id, value_index});
          }
        } else if (_scan_type == ScanType::OpLessThanEquals) {
          if (value_segment->get(value_index) <= type_cast<ColumnType>(_search_value)) {
            filtered_position.push_back(RowID{chunk_id, value_index});
          }
        } else if (_scan_type == ScanType::OpGreaterThan) {
          if (value_segment->get(value_index) > type_cast<ColumnType>(_search_value)) {
            filtered_position.push_back(RowID{chunk_id, value_index});
          }
        } else if (_scan_type == ScanType::OpGreaterThanEquals) {
          if (value_segment->get(value_index) >= type_cast<ColumnType>(_search_value)) {
            filtered_position.push_back(RowID{chunk_id, value_index});
          }
        }
      }
    }
     else if (auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnType>>(target_segment); dictionary_segment) {
      auto search_vid_low = dictionary_segment->lower_bound(_search_value);
      auto search_vid_upp = dictionary_segment->upper_bound(_search_value);

      auto filtered_position_op = std::vector<RowID>();
      auto filtered_position_equals = std::vector<RowID>();
      for (auto col_vid = ChunkID{0}; col_vid < dictionary_segment->size(); col_vid++) {
        if (dictionary_segment->attribute_vector()->get(col_vid) < search_vid_low &&
            (_scan_type == ScanType::OpLessThanEquals || _scan_type == ScanType::OpLessThan)) {
          filtered_position_op.push_back(RowID{chunk_id, col_vid});
        }
        if (dictionary_segment->attribute_vector()->get(col_vid) < search_vid_upp &&
            (_scan_type == ScanType::OpGreaterThanEquals || _scan_type == ScanType::OpGreaterThan)) {
          filtered_position_op.push_back(RowID{chunk_id, col_vid});
        }
        if (dictionary_segment->get_typed_value(dictionary_segment->attribute_vector()->get(col_vid)) == type_cast<ColumnType>(_search_value) &&
            _scan_type == ScanType::OpEquals){
          filtered_position_equals.push_back(RowID{chunk_id, col_vid});
        }
        if (dictionary_segment->get_typed_value(dictionary_segment->attribute_vector()->get(col_vid)) != type_cast<ColumnType>(_search_value) &&
          _scan_type == ScanType::OpNotEquals) {
          filtered_position.push_back(RowID{chunk_id, col_vid});
        }
      }
      if (_scan_type == ScanType::OpGreaterThanEquals || _scan_type == ScanType::OpLessThanEquals){
          std::merge(filtered_position_equals.begin(), filtered_position_equals.end(),
                                       filtered_position_op.begin(), filtered_position_op.end(), filtered_position.begin());
      }
      if (_scan_type == ScanType::OpEquals){
          filtered_position = filtered_position_equals;
      }
    } else if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(target_segment); reference_segment) {

    } else {
      Fail("Unknown segment type encountered.");
    }
  });
  return std::shared_ptr<const PosList>(&filtered_position);
}
}  // namespace opossum
