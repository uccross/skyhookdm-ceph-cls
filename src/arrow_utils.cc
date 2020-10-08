#include "arrow_utils.h"


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
int convert_arrow_to_buffer(const std::shared_ptr<arrow::Table> &table,
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
int create_arrow_table(std::shared_ptr<arrow::Table> *out_table) {
    
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