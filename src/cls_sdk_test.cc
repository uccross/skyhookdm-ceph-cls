#include <iostream>
#include <errno.h>

#include "test_utils.h"
#include "gtest/gtest.h"

using namespace librados;


TEST(ClsSDK, TestSDKCoverageWrite) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.exec("test_object", "cls_sdk", "test_coverage_write", in, out));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestSDKCoverageReplay) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  ASSERT_EQ(0, ioctx.exec("test_object", "cls_sdk", "test_coverage_write", in, out));
  ASSERT_EQ(0, ioctx.exec("test_object", "cls_sdk", "test_coverage_replay", in, out));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestCreateFragment) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  bufferlist in, out;
  // create an object first,then call exec on our method for that obj
  ASSERT_EQ(0, ioctx.exec("test_object", "cls_sdk", "test_create_fragment", in, out));
  ASSERT_EQ(0, ioctx.exec("test_object", "cls_sdk", "test_read_fragment", in, out));
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
