#include <rados/librados.hpp>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

int create_test_arrow_table(std::shared_ptr<arrow::Table> *out_table);

arrow::Status int64_to_char(uint8_t *num_buffer, int64_t num);
arrow::Status char_to_int64(uint8_t *num_buffer, int64_t &num);
arrow::Status deserialize_scan_request_from_bufferlist(std::shared_ptr<arrow::dataset::Expression> *filter, std::shared_ptr<arrow::Schema> *schema, librados::bufferlist bl);
arrow::Status serialize_scan_request_to_bufferlist(std::shared_ptr<arrow::dataset::Expression> filter, std::shared_ptr<arrow::Schema> schema, librados::bufferlist &bl);
