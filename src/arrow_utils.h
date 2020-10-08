#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>


int convert_arrow_to_buffer(const std::shared_ptr<arrow::Table> &table,
                            std::shared_ptr<arrow::Buffer> *buffer);
                            
int extract_arrow_from_buffer(std::shared_ptr<arrow::Table> *table,
                              const std::shared_ptr<arrow::Buffer> &buffer);

int create_arrow_table(std::shared_ptr<arrow::Table> *out_table);