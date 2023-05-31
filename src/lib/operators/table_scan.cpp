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
  const auto chunk_count = input_table->chunk_count();
  const auto column_count = input_table->column_count();

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
  output_chunks.reserve(chunk_count);
  for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
    const auto filter_column_type = input_table->column_type(_column_id);
    const auto input_chunk = input_table->get_chunk(chunk_index);
    const auto rows_matching_filter = _filter(filter_column_type, input_chunk, chunk_index);

    if (rows_matching_filter->empty()) {
      // No need to add an empty chunk in our output table.
      continue;
    }

    auto output_chunk = std::make_shared<Chunk>();
    // Even though we have only performed the filter on one column, the output table should obviously still retain
    // complete rows. For this reason, we need to construct a reference segment for each column.
    if (_input_table_is_actual_table()) {
      auto position_list = std::make_shared<PosList>();
      position_list->reserve(rows_matching_filter->size());
      // Because we know that the input_table is the "owner" of the data, all row indices matching our filter translate
      // directly to indices in the input_table segments.
      // Furthermore, we know that all output_segments can share the same position_list.
      for (auto row_index : *rows_matching_filter) {
        position_list->push_back(RowID{chunk_index, row_index});
      }
      for (auto column_index = ColumnID{0}; column_index < column_count; ++column_index) {
        output_chunk->add_segment(std::make_shared<ReferenceSegment>(input_table, column_index, position_list));
      }
    } else {
      // As we are dealing with a derived table as our input_table, we can't be sure that the segments of our
      // output table can share the same position list.
      // However, for input segments have shared the same position list, we know that the corresponding output segments
      // will be able to share the same position list as well.
      // We can ensure this by constructing a map mapping old position list identities (pointers, in other words)
      // to new position lists.
      auto new_position_lists = std::unordered_map<const PosList*, std::shared_ptr<PosList>>{};

      for (auto column_index = ColumnID{0}; column_index < column_count; ++column_index) {
        const auto reference_segment =
            std::dynamic_pointer_cast<ReferenceSegment>(input_chunk->get_segment(column_index));

        std::shared_ptr<PosList> new_position_list;
        if (new_position_lists.contains(reference_segment->pos_list().get())) {
          // We have already constructed a new position list for this old position list.
          new_position_list = new_position_lists.at(reference_segment->pos_list().get());
        } else {
          const auto old_position_list = reference_segment->pos_list();
          new_position_list = std::make_shared<PosList>();
          for (auto row_index : *rows_matching_filter) {
            // We need to make sure to reference the "original" segment in our output instead of the reference segment.
            // This is because we want to avoid constructing an indirection chain of
            // reference segments referencing reference segments referencing reference segments and so on
            new_position_list->push_back(old_position_list->at(row_index));
          }
        }

        output_chunk->add_segment(std::make_shared<ReferenceSegment>(
            reference_segment->referenced_table(), reference_segment->referenced_column_id(), new_position_list));
      }
    }

    output_chunks.push_back(output_chunk);
  }

  return std::make_shared<Table>(input_table, output_chunks);
}

bool TableScan::_input_table_is_actual_table() {
  if (_left_input_table()->chunk_count() == 0 || _left_input_table()->column_count() == 0) {
    return true;
  }

  const auto test_segment = _left_input_table()->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  // For a table, it holds by contract that either ALL segments are reference segments or ALL segments are not.
  // (Mixed reference/not-reference tables don't make sense because we don't want to copy any data if we don't need to).
  // Therefore, if casting our test_segment to a reference_segment succeeds, we know that we deal with a derived table.
  return !std::dynamic_pointer_cast<ReferenceSegment>(test_segment);
}

std::shared_ptr<const std::vector<ChunkOffset>> TableScan::_filter(const std::string& column_type,
                                                                   std::shared_ptr<const Chunk> chunk,
                                                                   ChunkID& chunk_id) {
  const auto target_segment = chunk->get_segment(_column_id);
  const auto segment_size = target_segment->size();
  auto filtered_positions = std::make_shared<std::vector<ChunkOffset>>();
  filtered_positions->reserve(segment_size);

  const auto segment_filter_function = _filter_function_for_segment(column_type, target_segment);
  for (auto row_index = ChunkOffset{0}; row_index < segment_size; ++row_index) {
    if (segment_filter_function(row_index)) {
      // Iff the filter function returns true, the row is matched by the filter.
      filtered_positions->push_back(row_index);
    }
  }

  filtered_positions->shrink_to_fit();
  return filtered_positions;
}

std::function<bool(ChunkOffset)> TableScan::_filter_function_for_segment(
    const std::string& column_type, const std::shared_ptr<AbstractSegment>& target_segment) {
  // In order to avoid having to perform the pointer casting and the switch on the scan type every time we want to
  // construct a filter function for a segment (which would need to happen on every row of a ReferenceSegment!),
  // we store our finished filter functions in a map so that we have to construct them only once for each
  // segment.
  // Note that we use raw AbstractSegment pointers as the map key, as we want to avoid problems with different
  // shared_ptr copies to the same segment being treated as different keys. This appears not to be a problem in
  // practice, but it feels like we shouldn't rely on that.
  if (!_filter_functions.contains(target_segment.get())) {
    resolve_data_type(column_type, [this, &column_type, &target_segment](auto type) {
      using ColumnType = typename decltype(type)::type;
      Assert(typeid(ColumnType) == _search_value.type(),
             "Search value doesn't have the same type as the column, we want to compare it with.");
      const auto _typed_search_value = type_cast<ColumnType>(_search_value);

      std::function<bool(ChunkOffset)> filter_function;
      if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(target_segment); reference_segment) {
        filter_function = [this, &column_type, reference_segment](auto row_index) {
          const auto row = reference_segment->pos_list()->operator[](row_index);
          // As every single row of the reference segment might point to a different chunk,
          // we need to retrieve the actual segment we are targeting and invoke its filter function
          // when making a filter decision for the reference segment. This filter function can be retrieved easily
          // by a recursive call to this method.
          // Note that this per-row filter-function construction is the reason we use a map to cache the filter
          // functions.
          const auto actual_target_segment = reference_segment->referenced_table()
                                                 ->get_chunk(row.chunk_id)
                                                 ->get_segment(reference_segment->referenced_column_id());
          return this->_filter_function_for_segment(column_type, actual_target_segment)(row.chunk_offset);
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
          // When filtering, NULL_VALUE should never be matched.
          return !value_segment->is_null(row_index) && comparator(value_segment->get(row_index), _typed_search_value);
        };
      } else if (auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnType>>(target_segment);
                 dictionary_segment) {
        const auto search_value_id_low = dictionary_segment->lower_bound(_search_value);
        const auto search_value_id_upp = dictionary_segment->upper_bound(_search_value);
        std::function<bool(ValueID)> comparator;
        switch (_scan_type) {
          case ScanType::OpEquals:
            if (search_value_id_low != search_value_id_upp) {
              // By the definition of lower_bound and upper_bound: If they are not equal, the search value has to be
              // located exactly at the lower_bound index...
              comparator = [search_value_id_low](auto search_value_id) {
                return search_value_id == search_value_id_low;
              };
            } else {
              // ... if they are equal, however, we know that our search value is not contained in the dictionary
              // and at the lower_bound index sits the first value > search_value.
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
          // When filtering, NULL_VALUE should never be matched.
          const auto row_value_id = dictionary_segment->attribute_vector()->get(row_index);
          return row_value_id != dictionary_segment->null_value_id() && comparator(row_value_id);
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
