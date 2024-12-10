FROM registry.deckhouse.io/base_images/golang:1.23.1-bullseye@sha256:a24507d1a36fce86431198a979435dadb187e8d0ce0b5c181f46d6788d84a40f
ARG PRIVATE_REPO_TOKEN
ENV PRIVATE_REPO_TOKEN ${PRIVATE_REPO_TOKEN}
RUN apt-get update && apt-get install -y apt-utils libbtrfs-dev file git gcc
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/bin
RUN git config --global url."https://gitlab-ci-token:$PRIVATE_REPO_TOKEN@fox.flant.com/".insteadOf https://fox.flant.com/
RUN git config --global --add safe.directory '*'
