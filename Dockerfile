FROM        ubuntu:jammy-20220801
LABEL       version="1.0"
ENV         DEBIAN_FRONTEND noninteractive

# update and install dependencies
RUN   apt-get -y update && apt-get -y install build-essential \
      && apt-get -y install zip unzip git cmake valgrind clang clang-tidy clang-format googletest zlib1g-dev libgflags-dev libbenchmark-dev libgtest-dev zsh curl git-all

RUN   sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" --unattended