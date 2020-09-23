/*
 * This is an example RADOS object class built using only the Ceph SDK
 * interface.
 */
#include "rados/objclass.h"
#include <arrow/api.h>
#include <arrow/dataset/api.h>

#include <vector>

CLS_VER(1, 0)
CLS_NAME(cls_sdk)

cls_handle_t h_class;
cls_method_handle_t h_test_coverage_write;
cls_method_handle_t h_test_coverage_replay;
cls_method_handle_t h_create_fragment;

static int create_fragment(cls_method_context_t hctx, ceph::buffer::list *in,
                           ceph::buffer::list *out) {
  // create the object
  int ret = cls_cxx_create(hctx, false);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_create returned %d", __func__, ret);
    return ret;
  }
  // read the object's data
  ceph::buffer::list bl;
  ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0)
    return ret;
  CLS_LOG(0, "data read=%s", bl.c_str(), std::to_string(ret).c_str());

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

  // create table schema
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))};
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::shared_ptr<arrow::Table> table =
      arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});
  if (table == nullptr) {
    CLS_LOG(0, "ERROR: Failed to create arrow table");
  }

  // Convert an arrow Table to InMemoryFragment 
  // Use the TableBatchReader to convert it to record batchses first
  std::shared_ptr<arrow::TableBatchReader> tableBatchReader =
    std::make_shared<arrow::TableBatchReader>(*table);

  std::shared_ptr<arrow::Schema> tableSchema = tableBatchReader->schema();

  arrow::RecordBatchVector recordBatchVector;

  if (!tableBatchReader->ReadAll(&recordBatchVector).ok()) {
    CLS_LOG(0, "ERROR: Failed to read as record batches vector");
  }
  // create fragment from table schema and recordbatch vector
  std::shared_ptr<arrow::dataset::InMemoryFragment> fragment =
      std::make_shared<arrow::dataset::InMemoryFragment>(tableSchema,
                                                         recordBatchVector);
 
 
 return 0;
}

/**
 * test_coverage_write - a "write" method that creates an object
 *
 * This method modifies the object by making multiple write calls (write,
 * setxattr and set_val).
 */
static int test_coverage_write(cls_method_context_t hctx,
                               ceph::buffer::list *in,
                               ceph::buffer::list *out) {
  // create the object
  int ret = cls_cxx_create(hctx, false);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_create returned %d", __func__, ret);
    return ret;
  }

  uint64_t size;
  // get the size of the object
  ret = cls_cxx_stat(hctx, &size, NULL);
  if (ret < 0)
    return ret;

  std::string c = "test";
  ceph::buffer::list bl;
  bl.append(c);

  // write to the object
  ret = cls_cxx_write(hctx, 0, bl.length(), &bl);
  if (ret < 0)
    return ret;

  uint64_t new_size;
  // get the new size of the object
  ret = cls_cxx_stat(hctx, &new_size, NULL);
  if (ret < 0)
    return ret;

  // make some change to the xattr
  ret = cls_cxx_setxattr(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  // make some change to the omap
  ret = cls_cxx_map_set_val(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  return 0;
}

/**
 * test_coverage_replay - a "read" method to retrieve previously written data
 *
 * This method reads the object by making multiple read calls (read, getxattr
 * and get_val). It also removes the object after reading.
 */

static int test_coverage_replay(cls_method_context_t hctx,
                                ceph::buffer::list *in,
                                ceph::buffer::list *out) {
  CLS_LOG(0, "reading already written object");
  uint64_t size;
  // get the size of the object
  int ret = cls_cxx_stat(hctx, &size, NULL);
  if (ret < 0)
    return ret;

  ceph::buffer::list bl;
  // read the object entry
  ret = cls_cxx_read(hctx, 0, size, &bl);
  if (ret < 0)
    return ret;

  // if the size is incorrect
  if (bl.length() != size)
    return -EIO;

  bl.clear();

  // read xattr entry
  ret = cls_cxx_getxattr(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  // if the size is incorrect
  if (bl.length() != size)
    return -EIO;

  bl.clear();

  // read omap entry
  ret = cls_cxx_map_get_val(hctx, "foo", &bl);
  if (ret < 0)
    return ret;

  // if the size is incorrect
  if (bl.length() != size)
    return -EIO;

  // remove the object
  ret = cls_cxx_remove(hctx);
  if (ret < 0)
    return ret;

  return 0;
}

CLS_INIT(cls_sdk) {
  CLS_LOG(0, "loading cls_sdk class");

  cls_register("cls_sdk", &h_class);

  cls_register_cxx_method(h_class, "test_coverage_write",
                          CLS_METHOD_RD | CLS_METHOD_WR, test_coverage_write,
                          &h_test_coverage_write);

  cls_register_cxx_method(h_class, "test_coverage_replay",
                          CLS_METHOD_RD | CLS_METHOD_WR, test_coverage_replay,
                          &h_test_coverage_replay);

    cls_register_cxx_method(h_class, "create_fragment",
    CLS_METHOD_RD | CLS_METHOD_WR, create_fragment,
    &h_create_fragment);
}
