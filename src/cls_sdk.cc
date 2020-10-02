/*
 * This is an example RADOS object class built using only the Ceph SDK
 * interface.
 */
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

/*
 * Function: convert_arrow_to_buffer
 * Description: Convert arrow table into record batches which are dumped on to a
 *              output buffer. For converting arrow table three things are essential.
 *  a. Where to write - In this case we are using buffer, but it can be streams or
 *                      files as well
 *  b. OutputStream - We connect a buffer (or file) to the stream and writer writes
 *                    data from this stream. We are using arrow::BufferOutputStream
 *                    as an output stream.
 *  c. Writer - Does writing data to OutputStream. We are using arrow::RecordBatchStreamWriter
 *              which will write the data to output stream.
 * @param[in] table   : Arrow table to be converted
 * @param[out] buffer : Output buffer
 * Return Value: error code
 */
static int convert_arrow_to_buffer(const std::shared_ptr<arrow::Table> &table,
                                   std::shared_ptr<arrow::Buffer> *buffer) {
  // Initialization related to writing to the the file
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  arrow::Result<std::shared_ptr<arrow::io::BufferOutputStream>> out;
  std::shared_ptr<arrow::io::BufferOutputStream> output;
  out = arrow::io::BufferOutputStream::Create();
  if (out.ok()) {
    output = out.ValueOrDie();
    arrow::io::OutputStream *raw_out = output.get();
    arrow::Table *raw_table = table.get();
    const arrow::ipc::IpcWriteOptions options =
        arrow::ipc::IpcWriteOptions::Defaults();
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>> result =
        arrow::ipc::NewStreamWriter(raw_out, raw_table->schema(), options);
    if (result.ok()) {
      writer = std::move(result).ValueOrDie();
    }
  }

  // Initialization related to reading from arrow
  writer->WriteTable(*(table.get()));
  writer->Close();
  arrow::Result<std::shared_ptr<arrow::Buffer>> buff;
  buff = output->Finish();
  if (buff.ok()) {
    *buffer = buff.ValueOrDie();
  }
  return 0;
}

/*
 * Function: extract_arrow_from_buffer
 * Description: Extract arrow table from a buffer.
 *  a. Where to read - In this case we are using buffer, but it can be streams or
 *                     files as well.
 *  b. InputStream - We connect a buffer (or file) to the stream and reader will read
 *                   data from this stream. We are using arrow::InputStream as an
 *                   input stream.
 *  c. Reader - Reads the data from InputStream. We are using arrow::RecordBatchReader
 *              which will read the data from input stream.
 * @param[out] table  : Arrow table to be converted
 * @param[in] buffer  : Input buffer
 * Return Value: error code
 */
int extract_arrow_from_buffer(std::shared_ptr<arrow::Table> *table,
                              const std::shared_ptr<arrow::Buffer> &buffer) {
  // Initialization related to reading from a buffer
  const std::shared_ptr<arrow::io::InputStream> buf_reader =
      std::make_shared<arrow::io::BufferReader>(buffer);
  std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
  arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>> result =
      arrow::ipc::RecordBatchStreamReader::Open(buf_reader);
  if (result.ok()) {
    reader = std::move(result).ValueOrDie();
  }

  auto schema = reader->schema();
  auto metadata = schema->metadata();

  // this check crashes if num_rows not in metadata (e.g., HEP array tables)
  // if (atoi(metadata->value(METADATA_NUM_ROWS).c_str()) == 0)

  // Initilaization related to read to apache arrow
  std::vector<std::shared_ptr<arrow::RecordBatch>> batch_vec;
  while (true) {
    std::shared_ptr<arrow::RecordBatch> chunk;
    reader->ReadNext(&chunk);
    if (chunk == nullptr)
      break;
    batch_vec.push_back(chunk);
  }

  if (batch_vec.size() == 0) {
    // Table has no values and current version of Apache Arrow does not allow
    // converting empty recordbatches to arrow table.
    // https://www.mail-archive.com/issues@arrow.apache.org/msg30289.html
    // Therefore, create and return empty arrow table
    std::vector<std::shared_ptr<arrow::Array>> array_list;
    *table = arrow::Table::Make(schema, array_list);
  } else {
    // https://github.com/apache/arrow/blob/master/cpp/src/arrow/table.cc#L280
    // The num_chunks in each columns of the result table is decided by
    // batch_vec size().
    arrow::Result<std::shared_ptr<arrow::Table>> result =
        arrow::Table::FromRecordBatches(batch_vec);
    if (result.ok()) {
      *table = std::move(result).ValueOrDie();
    }
  }
  return 0;
}


/*
 * Function: create_arrow_table
 * Description: Make an arrow table using faked data.
 * Need arrow memory_pool, arrayBuilder, nested arrayBuilder
 */
static int create_arrow_table(std::shared_ptr<arrow::Table> *out_table) {
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
  *out_table =
      arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});
  if (*out_table == nullptr) {
    return -1;
  }
  return 0;
}

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
