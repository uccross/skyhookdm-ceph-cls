ARG CEPH_RELEASE=v15.2.4
FROM ceph/ceph:${CEPH_RELEASE}

ARG ARROW_RPM_REPO_URL="https://apache.bintray.com/arrow/centos/8/apache-arrow-release-latest.rpm"

RUN dnf group install -y 'Development Tools' && \
    dnf install -y cmake rados-objclass-devel && \
    dnf install -y ${ARROW_RPM_REPO_URL} && \
    dnf install -y --enablerepo=epel --enablerepo=PowerTools \
       arrow-dataset-devel-1.0.0-1.el8 && \
    dnf clean all
