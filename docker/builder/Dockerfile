ARG CEPH_RELEASE=v15.2.4
FROM ceph/ceph:${CEPH_RELEASE}


RUN dnf group install -y 'Development Tools' && \
    dnf install -y cmake rados-objclass-devel git && \
    dnf install -y --enablerepo=epel --enablerepo=PowerTools gtest-devel && \
    dnf clean all

RUN git clone https://github.com/apache/arrow && \
    cd arrow/cpp && \
    mkdir -p debug && cd debug && \
    cmake -DCMAKE_BUILD_TYPE=Debug -DARROW_BUILD_TESTS=ON -DARROW_CSV=ON -DARROW_DATASET=ON .. && \
    make -j8 install
