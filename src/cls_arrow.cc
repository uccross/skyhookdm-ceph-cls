#include "rados/objclass.h"
#include "cls_arrow_utils.h"

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

CLS_VER(1, 0)
CLS_NAME(arrow)

cls_handle_t h_class;
cls_method_handle_t h_read;
cls_method_handle_t h_write;
cls_method_handle_t h_test_create;
cls_method_handle_t h_test_read;

static int write(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out) {
  int ret;

  ret = cls_cxx_create(hctx, false);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to create an object");
    return ret;
  }

  ret = cls_cxx_write(hctx, 0, in->length(), in);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to write to object");
    return ret;
  }

  return 0;
}

static int read(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out) {
  int ret;
  arrow::Status arrow_ret;
  
  std::shared_ptr<arrow::dataset::Expression> filter;
  std::shared_ptr<arrow::Schema> schema;

  arrow_ret = deserialize_scan_request_from_bufferlist(&filter, &schema, *in);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: Failed to extract arrow::dataset::Expression and arrow::Schema");
    return -1;
  }

  ceph::buffer::list bl;
  ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to read an object");
    return ret;
  }

  arrow::RecordBatchVector batches;
  arrow_ret = extract_batches_from_bufferlist(&batches, bl);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: Failed to extract arrow::RecordBatchVector from ceph::buffer::list");
    return -1;
  }

  std::shared_ptr<arrow::Table> result_table;
  arrow_ret = scan_batches(schema, batches, &result_table);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: Failed to scan arrow::RecordBatchVector");
    return -1;
  }

  ceph::buffer::list result_bl;
  arrow_ret = write_table_to_bufferlist(result_table, result_bl);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: Failed to write arrow::Table to ceph::buffer::list");
    return -1;
  }

  *out = result_bl;  
  return 0;
}

static int test_create(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out) {
  // Create a test object
  int ret;
  arrow::Status arrow_ret;

  ret = cls_cxx_create(hctx, false);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to create an object");
    return ret;
  }

  // Generate the test arrow table
  std::shared_ptr<arrow::Table> table;
  ret = create_test_arrow_table(&table);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to create a test arrow::Table");
    return ret;
  }

  // Write the test arrow table to a bufferlist
  ceph::buffer::list bl;  
  arrow_ret = write_table_to_bufferlist(table, bl);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: Failed to write arrow::Table to ceph::buffer::list");
    return -1;
  }

  // Write bufferlist to the object
  ret = cls_cxx_write(hctx, 0, bl.length(), &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to write to object");
    return ret;
  }

  return 0;
}

static int test_read(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out) {
  int ret;
  arrow::Status arrow_ret;

  // LHS
  std::shared_ptr<arrow::Table> table;
  ret = create_test_arrow_table(&table);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to create the test arrow::Table");
    return ret;
  }
  auto filter = arrow::dataset::scalar(true);
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))});

  // RHS
  std::shared_ptr<arrow::dataset::Expression> filter_;
  std::shared_ptr<arrow::Schema> schema_;
  arrow_ret = deserialize_scan_request_from_bufferlist(&filter_, &schema_, *in);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: Failed to extract arrow::dataset::Expression and arrow::Schema");
    return -1;
  }

  ceph::buffer::list bl_;
  ret = cls_cxx_read(hctx, 0, 0, &bl_);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to read an object");
    return ret;
  }

  arrow::RecordBatchVector batches_;
  arrow_ret = extract_batches_from_bufferlist(&batches_, bl_);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: Failed to extract arrow::RecordBatchVector from ceph::buffer::list");
    return -1;
  }

  std::shared_ptr<arrow::Table> table_;
  arrow_ret = scan_batches(schema_, batches_, &table_);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: Failed to scan arrow::RecordBatchVector");
    return -1;
  }

  // Checks
  if (!table->Equals(*table_)) {
    CLS_ERR("ERROR: Table read from bufferlist is not the same as expected");
    return -1;
  }
  if (!schema->Equals(*schema_)) {
    CLS_ERR("ERROR: Schema read from bufferlist is not the same as expected");
    return -1;
  }
  if (!filter->Equals(*filter_)) {
    CLS_ERR("ERROR: Expression read from bufferlist is not the same as expected");
    return -1;
  }

  return 0;
}

CLS_INIT(arrow)
{
  CLS_LOG(0, "loading cls_arrow");

  cls_register("arrow", &h_class);

  cls_register_cxx_method(h_class, "read",
                          CLS_METHOD_RD | CLS_METHOD_WR, read,
                          &h_read);

  cls_register_cxx_method(h_class, "write",
                          CLS_METHOD_RD | CLS_METHOD_WR, write,
                          &h_write);

  cls_register_cxx_method(h_class, "test_create",
                          CLS_METHOD_RD | CLS_METHOD_WR, test_create,
                          &h_test_create);

  cls_register_cxx_method(h_class, "test_read",
                          CLS_METHOD_RD | CLS_METHOD_WR, test_read,
                          &h_test_read);
}
