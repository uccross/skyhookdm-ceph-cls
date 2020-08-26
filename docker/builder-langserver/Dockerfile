FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive

ARG ARROW_VERSION=1.0.1-1

RUN apt update && \
    # install build tools \
    apt install -y \
       curl clang clangd cmake ninja-build && \
    # install ceph libs \
    apt install -y --no-install-recommends \
       ceph-osd ceph-mon libradospp-dev rados-objclass-dev && \
    # install arrow \
    curl -LO \
       https://apache.bintray.com/arrow/ubuntu/apache-arrow-archive-keyring-latest-focal.deb && \
    apt install -y \
       ./apache-arrow-archive-keyring-latest-focal.deb && \
    apt update && \
    apt install -y \
       libarrow-dataset-dev=${ARROW_VERSION} && \
    # install neovim and its python API \
    apt install -y \
       git neovim python3-neovim && \
    # install node for coc.nvim \
    curl -sL https://deb.nodesource.com/setup_14.x | bash - && \
    apt update && \
    apt install -y nodejs && \
    # cleanup apt cache \
    rm -rf /var/lib/apt/lists/* && \
    # configure neovim to read regular .vimrc files \
    mkdir -p /root/.config/nvim/ && \
    echo 'set runtimepath^=~/.vim runtimepath+=~/.vim/after' > /root/.config/nvim/init.vim && \
    echo 'let &packpath=&runtimepath' >> /root/.config/nvim/init.vim && \
    echo 'source ~/.vimrc' >> /root/.config/nvim/init.vim

# opinionated vim config
COPY vimrc /root/.vimrc
