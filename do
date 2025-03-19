#!/usr/bin/env bash

COMMAND="$1"

case "$COMMAND" in
  build-dev-docker)
    echo "Building Docker image 'aito-python-tools'..."
    docker build -t aito-python-tools -f Dockerfile.dev .
    ;;

  run-dev-docker)
    echo "Running Docker container from 'aito-python-tools' image..."
    docker run -it --rm \
      -e DISPLAY=$DISPLAY \
      -v /tmp/.X11-unix:/tmp/.X11-unix \
      -e TERM=xterm-256color \
      -v "$(pwd)":/workspace \
      --network host \
      aito-python-tools \
      bash
    ;;

  *)
    echo "Usage: $0 {build-dev-docker|run-dev-docker}"
    exit 1
    ;;
esac
