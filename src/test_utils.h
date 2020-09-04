#include <rados/librados.hpp>
#include<iostream> 
#include<string>
#include <sstream>

using namespace librados;

std::string create_one_pool_pp(const std::string &pool_name,
			    librados::Rados &cluster);
std::string create_one_pool_pp(const std::string &pool_name,
			       librados::Rados &cluster,
			       const std::map<std::string, std::string> &config);
std::string create_one_ec_pool_pp(const std::string &pool_name,
			    librados::Rados &cluster);
std::string connect_cluster_pp(librados::Rados &cluster);
std::string connect_cluster_pp(librados::Rados &cluster,
			       const std::map<std::string, std::string> &config);
int destroy_one_pool_pp(const std::string &pool_name, librados::Rados &cluster);
