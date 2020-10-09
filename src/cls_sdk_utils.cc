#include "cls_sdk_utils.h"

int create_test_arrow_table(std::shared_ptr<arrow::Table> *out_table) {
    
  // create a memory pool
  arrow::MemoryPool *pool = arrow::default_memory_pool();
    
  // arrow array builder for each table column
  arrow::Int32Builder id_builder(pool);
  arrow::DoubleBuilder cost_builder(pool);
  arrow::ListBuilder components_builder(
      pool, std::make_shared<arrow::DoubleBuilder>(pool));
    
  // The following builder is owned by components_builder.
  arrow::DoubleBuilder &cost_components_builder = *(
      static_cast<arrow::DoubleBuilder *>(components_builder.value_builder()));

  // append some fake data
  for (int i = 0; i < 10; ++i) {
    id_builder.Append(i);
    cost_builder.Append(i + 1.0);
    // Indicate the start of a new list row. This will memorise the current
    // offset in the values builder.
    components_builder.Append();
    std::vector<double> nums;
    nums.push_back(i + 1.0);
    nums.push_back(i + 2.0);
    nums.push_back(i + 3.0);
    cost_components_builder.AppendValues(nums.data(), nums.size());
  }
  
  // At the end, we finalise the arrays, declare the (type) schema and combine
  // them into a single `arrow::Table`:
  std::shared_ptr<arrow::Int32Array> id_array;
  id_builder.Finish(&id_array);
  std::shared_ptr<arrow::DoubleArray> cost_array;
  cost_builder.Finish(&cost_array);
  
  // No need to invoke cost_components_builder.Finish because it is implied by
  // the parent builder's Finish invocation.
  std::shared_ptr<arrow::ListArray> cost_components_array;
  components_builder.Finish(&cost_components_array);

  // create table schema and make table from col arrays and schema
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))};
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  *out_table =
      arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});
  if (*out_table == nullptr) {
    return -1;
  }
  return 0;
}
