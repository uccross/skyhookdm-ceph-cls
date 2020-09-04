#include <iostream>
#include <errno.h>

#include "test_utils.h"
#include "gtest/gtest.h"

using namespace librados;


TEST(ClsSDK, TestSDKCoverageWrite) {
  Rados cluster;
  //std::string pool_name = get_temp_pool_name();
  std::string pool_name = "testPool";
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.exec("myobject", "arrow_cls", "test_coverage_write", in, out));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestSDKCoverageReplay) {
  Rados cluster;
  //std::string pool_name = get_temp_pool_name();
  std::string pool_name = "testPool";
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.exec("myobject", "arrow_cls", "test_coverage_write", in, out));
  ASSERT_EQ(0, ioctx.exec("myobject", "arrow_cls", "test_coverage_replay", in, out));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
