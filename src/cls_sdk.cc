#include "rados/objclass.h"
#include "cls_sdk_utils.h"

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

CLS_VER(1, 0)
CLS_NAME(cls_sdk)

cls_handle_t h_class;
cls_method_handle_t h_read;
cls_method_handle_t h_test_create_fragment;
cls_method_handle_t h_test_read_fragment;

static arrow::Status extract_batches_from_bufferlist(std::shared_ptr<arrow::Schema> *schema, arrow::RecordBatchVector *batches, ceph::buffer::list &bl) {
  std::shared_ptr<arrow::Buffer> buffer = std::make_shared<arrow::Buffer>((uint8_t*)bl.c_str(), bl.length());
  std::shared_ptr<arrow::io::BufferReader> buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader, arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));
  ARROW_RETURN_NOT_OK(record_batch_reader->ReadAll(batches));
  *schema = record_batch_reader->schema();
  return arrow::Status::OK();
}

static arrow::Status write_table_to_bufferlist(std::shared_ptr<arrow::Table> &table, ceph::buffer::list &bl) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, arrow::io::BufferOutputStream::Create());
  const auto options = arrow::ipc::IpcWriteOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::NewStreamWriter(buffer_output_stream.get(), table->schema(), options));

  writer->WriteTable(*table);
  writer->Close();

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return arrow::Status::OK();
}

static arrow::Status scan_batches(std::shared_ptr<arrow::Schema> &schema, arrow::RecordBatchVector &batches, std::shared_ptr<arrow::Table> *table) {
  std::shared_ptr<arrow::dataset::ScanContext> scan_context = std::make_shared<arrow::dataset::ScanContext>();
  std::shared_ptr<arrow::dataset::InMemoryFragment> fragment = std::make_shared<arrow::dataset::InMemoryFragment>(batches);
  std::shared_ptr<arrow::dataset::ScannerBuilder> builder = std::make_shared<arrow::dataset::ScannerBuilder>(schema, fragment, scan_context);
  
  ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
  ARROW_ASSIGN_OR_RAISE(auto table_, scanner->ToTable());

  *table = table_;
  return arrow::Status::OK();
}

static int read(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out) {
  ceph::buffer::list bl;
  int ret;
  
  ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to read an object");
    return ret;
  }

  arrow::RecordBatchVector batches;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status arrow_ret;
  
  arrow_ret = extract_batches_from_bufferlist(&schema, &batches, bl);
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

static int test_create_fragment(cls_method_context_t hctx, ceph::buffer::list *in, ceph::buffer::list *out) {
  // Create a test object
  int ret;

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
  arrow::Status arrow_ret;
  
  arrow_ret = write_table_to_bufferlist(table, bl);
  if (!arrow_ret.ok()) {
    CLS_ERR("ERROR: Failed to write arrow::Table to ceph::buffer::list");
    return -1;
  }

  // Write bufferlist to the object
  ret = cls_cxx_write(hctx, 0, bl.length(), &bl);
  if (ret < 0) {
    CLS_ERR("Failed to write to object");
    return ret;
  }

  return 0;
}

static int test_read_fragment(cls_method_context_t hctx, ceph::buffer::list *in,
                              ceph::buffer::list *out) {
  // Get the test arrow table
  std::shared_ptr<arrow::Table> table;
  int ret = create_test_arrow_table(&table);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to create the test arrow::Table");
    return ret;
  }

  // Read the object's data
  ceph::buffer::list bl;
  ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: Failed to read an object");
    return ret;
  }

  // Read a vector of RecordBatches from bufferlist
  arrow::RecordBatchVector batches;
  std::shared_ptr<arrow::Schema> schema;
  arrow::Status arrow_ret;

  arrow_ret = extract_batches_from_bufferlist(&schema, &batches, bl);
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

  // Check whether the table was read back correctly
  if (!table->Equals(*result_table)) {
    CLS_ERR("ERROR: Table read from bufferlist is not the same as expected");
    return -1;
  }

  return 0;
}

CLS_INIT(cls_sdk) {

  CLS_LOG(0, "loading cls_sdk class");

  cls_register("cls_sdk", &h_class);

  cls_register_cxx_method(h_class, "read",
                          CLS_METHOD_RD | CLS_METHOD_WR, read,
                          &h_read);

  cls_register_cxx_method(h_class, "test_create_fragment",
                          CLS_METHOD_RD | CLS_METHOD_WR, test_create_fragment,
                          &h_test_create_fragment);

  cls_register_cxx_method(h_class, "test_read_fragment",
                          CLS_METHOD_RD | CLS_METHOD_WR, test_read_fragment,
                          &h_test_read_fragment);
}
