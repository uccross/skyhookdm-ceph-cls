ARG CEPH_RELEASE="v14.2.9"
FROM ceph/ceph:${CEPH_RELEASE} as build

# gcc, cmake3, rados sdk, re2
RUN yum install -y \
      centos-release-scl \
      devtoolset-8-gcc \
      cmake3 \
      boost-devel \
      rados-objclass-devel \
      re2-devel && \
    # arrow, parquet, boost \
    yum install -y https://apache.bintray.com/arrow/centos/7/apache-arrow-release-latest.rpm && \
    yum install -y --enablerepo=epel \
      arrow-devel \
      parquet-devel && \
    yum clean all

FROM ceph/ceph:${CEPH_RELEASE} as release
