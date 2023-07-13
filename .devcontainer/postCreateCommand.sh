#!/bin/sh
# postCreateCommand.sh

echo "START Install"

sudo apt update
sudo apt install git-lfs -y
sudo chown -R vscode:vscode .

poetry config virtualenvs.in-project true
poetry install --no-root

echo "FINISH Install"