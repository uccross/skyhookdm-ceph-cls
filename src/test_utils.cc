// TOOD
#include "test_utils.h"

std::string create_one_pool_pp(const std::string &pool_name, Rados &cluster)
{
    return create_one_pool_pp(pool_name, cluster, {});
}
std::string create_one_pool_pp(const std::string &pool_name, Rados &cluster,
                               const std::map<std::string, std::string> &config)
{
  std::string err = connect_cluster_pp(cluster, config);
  if (err.length())
    return err;
  int ret = cluster.pool_create(pool_name.c_str());
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.pool_create(" << pool_name << ") failed with error " << ret;
    return oss.str();
  }

  IoCtx ioctx;
  ret = cluster.ioctx_create(pool_name.c_str(), ioctx);
  if (ret < 0) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.ioctx_create(" << pool_name << ") failed with error "
        << ret;
    return oss.str();
  }
  ioctx.application_enable("rados", true);
  return "";
}

int destroy_one_pool_pp(const std::string &pool_name, Rados &cluster)
{
  int ret = cluster.pool_delete(pool_name.c_str());
  if (ret) {
    cluster.shutdown();
    return ret;
  }
  cluster.shutdown();
  return 0;
}

std::string get_temp_pool_name(const std::string &prefix)
{
  char hostname[80];
  char out[160];
  memset(hostname, 0, sizeof(hostname));
  memset(out, 0, sizeof(out));
  gethostname(hostname, sizeof(hostname)-1);
  static int num = 1;
  snprintf(out, sizeof(out), "%s-%d-%d", hostname, getpid(), num);
  num++;
  return prefix + out;
}




