FROM mcr.microsoft.com/devcontainers/base:ubuntu

RUN apt-get update && apt-get install -y \
    fuse3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN echo 'user_allow_other' >> /etc/fuse.conf

RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
