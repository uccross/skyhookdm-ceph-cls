#include <iostream>
#include <errno.h>

#include "cls_arrow_utils.h"
#include "test_utils.h"
#include "gtest/gtest.h"


TEST(ClsSDK, TestCreateAndReadFragment) {
  librados::Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  librados::bufferlist in, out;

  auto filter = arrow::dataset::scalar(true);
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))});

  serialize_scan_request_to_bufferlist(filter, schema, in); 

  // Create an object containing a table and read back the table from the same object 
  ASSERT_EQ(0, ioctx.exec("test_object", "arrow", "test_create", in, out));
  ASSERT_EQ(0, ioctx.exec("test_object", "arrow", "test_read", in, out));
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
