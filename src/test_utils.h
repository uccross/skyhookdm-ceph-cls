// TODO add:
// get_temp_pool_name()
// create_one_pool_pp()
// destroy_one_pool_pp()
#include <rados/librados.hpp>
#include "gtest/gtest.h"
std::string create_one_pool_pp(const std::string &pool_name,
			    librados::Rados &cluster);
std::string create_one_pool_pp(const std::string &pool_name,
			       librados::Rados &cluster,
			       const std::map<std::string, std::string> &config);

int destroy_one_pool_pp(const std::string &pool_name, librados::Rados &cluster);

std::string get_temp_pool_name(const std::string &prefix = "test-rados-api-");
