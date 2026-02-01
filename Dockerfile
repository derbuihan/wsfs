FROM golang:1.22-bookworm

RUN apt-get update \
  && apt-get install -y fuse3 vim curl unzip \
  && rm -rf /var/lib/apt/lists/*

RUN echo 'user_allow_other' >> /etc/fuse.conf

# Install Databricks CLI
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

WORKDIR /work

COPY go.mod go.sum ./
RUN /usr/local/go/bin/go mod download

CMD ["bash", "./scripts/docker_fuse_test.sh"]
