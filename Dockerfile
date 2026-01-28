FROM golang:1.22-bookworm

RUN apt-get update \
  && apt-get install -y fuse3 vim \
  && rm -rf /var/lib/apt/lists/*

RUN echo 'user_allow_other' >> /etc/fuse.conf

WORKDIR /work

COPY go.mod go.sum ./
RUN /usr/local/go/bin/go mod download

CMD ["bash", "./scripts/docker_fuse_test.sh"]
