/*
 * This is an example RADOS object class built using only the Ceph SDK
 * interface.
 */
#include "arrow_utils.h"
#include "rados/objclass.h"
#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <vector>

CLS_VER(1, 0)
CLS_NAME(cls_sdk)

cls_handle_t h_class;
cls_method_handle_t h_test_coverage_write;
cls_method_handle_t h_test_coverage_replay;
cls_method_handle_t h_test_create_fragment;
cls_method_handle_t h_test_read_fragment;
/**
 * create_fragment
 *
 * This method created an arrow table with faked data, and convert the table to
 * InMemoryFragment, and then convert to ScannerBuilder. Filter and Projection
 * operation can be added to the arrow::dataset::ScannerBuilder.
 *
 * Then, we convert the ScannerBuilder back to arrow table and write it to Ceph
 * bufferlist and to object finally.
 */
static int test_create_fragment(cls_method_context_t hctx,
                                ceph::buffer::list *in,
                                ceph::buffer::list *out) {
  // create the object
  int ret = cls_cxx_create(hctx, false);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_create returned %d", __func__, ret);
    return ret;
  }

  std::shared_ptr<arrow::Table> table;
  ret = create_arrow_table(&table);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: Failed to create an arrow table");
    return ret;
  }

  // *********** Now we have the arrow table ***********************************

  // Convert an arrow Table to InMemoryFragment
  // Use the TableBatchReader to convert it to record batchses first
  std::shared_ptr<arrow::TableBatchReader> tableBatchReader =
      std::make_shared<arrow::TableBatchReader>(*table);
  std::shared_ptr<arrow::Schema> tableSchema = tableBatchReader->schema();

  arrow::RecordBatchVector recordBatchVector;
  if (!tableBatchReader->ReadAll(&recordBatchVector).ok()) {
    CLS_LOG(0, "ERROR: Failed to read as record batches vector");
    return -1;
  }
  // create fragment from table schema and recordbatch vector
  std::shared_ptr<arrow::dataset::InMemoryFragment> fragment =
      std::make_shared<arrow::dataset::InMemoryFragment>(tableSchema,
                                                         recordBatchVector);

  std::shared_ptr<arrow::dataset::ScanContext> scanContext =
      std::make_shared<arrow::dataset::ScanContext>();

  // create the ScannerBuilder and get the Scanner
  arrow::dataset::ScannerBuilder *scannerBuilder =
      new arrow::dataset::ScannerBuilder(tableSchema, fragment, scanContext);

  // ** Filter and Projection operation can be done here in ScannerBuilder *****

  arrow::Result<std::shared_ptr<arrow::dataset::Scanner>> result =
      scannerBuilder->Finish();

  // arrow::Result object is returned, result.ok() may not be necessary
  if (result.ok()) {
    // Now we have the Scanner, we can transform the Scanner to arrow table
    // again and write the raw data to Ceph buffer list
    std::shared_ptr<arrow::dataset::Scanner> scanner = result.ValueOrDie();
    std::shared_ptr<arrow::Table> out_table = scanner->ToTable().ValueOrDie();

    std::shared_ptr<arrow::Buffer> buffer;
    convert_arrow_to_buffer(out_table, &buffer);
      
    // write the data to bl
    ceph::buffer::list bl;
    bl.append((char *)buffer->data(), buffer->size());

    // write to the object
    ret = cls_cxx_write(hctx, 0, bl.length(), &bl);
    if (ret < 0)
      return ret;

    uint64_t size;
    // get the size of the object
    ret = cls_cxx_stat(hctx, &size, NULL);
    if (ret < 0)
      return ret;

    // if the size is incorrect
    if (bl.length() != size)
      return -EIO;

  } else {
    CLS_LOG(0, "ERROR: Failed to build arrow dataset Scanner");
    return -1;
  }

  return 0;
}

/**
 * test_read_fragment
 *
 * Read from the Ceph obj to get the bufferlist, and extract arrow buffer from it.
 * Then convert the buffer to arrow::table, and compare the table with origin one.
 */
static int test_read_fragment(cls_method_context_t hctx, ceph::buffer::list *in,
                              ceph::buffer::list *out) {
  // create the object
  int ret = cls_cxx_create(hctx, false);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_create returned %d", __func__, ret);
    return ret;
  }

  // get the origin arrow table
  std::shared_ptr<arrow::Table> table;
  ret = create_arrow_table(&table);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: Failed to create an arrow table");
    return ret;
  }

  // read the object's data
  ceph::buffer::list bl_read;
  ret = cls_cxx_read(hctx, 0, 0, &bl_read);
  if (ret < 0) {
    CLS_ERR("ERROR: read_fragment: %d", ret);
    return ret;
  }

  // todo: what if we write multiple tables to bl, how to split them?
  std::shared_ptr<arrow::Buffer> buffer_read = arrow::MutableBuffer::Wrap(
      reinterpret_cast<uint8_t *>(const_cast<char *>(bl_read.c_str())),
      bl_read.length());

  std::shared_ptr<arrow::Table> input_table;

  // Get input table from dataptr
  extract_arrow_from_buffer(&input_table, buffer_read);

  if (!input_table->Equals(*table)) {
    CLS_LOG(0, "ERROR: Table read from bl is not the same as expected");
    return -1;
  }
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

  cls_register_cxx_method(h_class, "test_create_fragment",
                          CLS_METHOD_RD | CLS_METHOD_WR, test_create_fragment,
                          &h_test_create_fragment);

  cls_register_cxx_method(h_class, "test_read_fragment",
                          CLS_METHOD_RD | CLS_METHOD_WR, test_read_fragment,
                          &h_test_read_fragment);
}
