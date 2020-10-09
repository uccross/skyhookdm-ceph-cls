#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

int create_test_arrow_table(std::shared_ptr<arrow::Table> *out_table);