#!/bin/bash

install_dependencies() {
  if [ -f "/opt/deus_dev_env/pyproject.toml" ]; then
    echo "Installing dependencies..."
    poetry install
  fi
}

# Initial install
install_dependencies

# Watch for changes and re-install dependencies
while true; do
  inotifywait -e modify,create,delete /opt/deus_dev_env/pyproject.toml /opt/deus_dev_env/poetry.lock
  install_dependencies
done

exec /bin/bash
