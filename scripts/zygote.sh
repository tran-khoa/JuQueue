#!/bin/bash

# Expects the following environmental variables
# ZYGOTE_DIR: working directory
# ZYGOTE_EXEC: script/program to be executed

if [ -z ${ZYGOTE_DIR+x} ]; then
  echo "ZYGOTE_DIR not set, exiting..."
  exit 1
fi
if [ -z ${ZYGOTE_EXEC+x} ]; then
  echo "ZYGOTE_EXEC not set, exiting..."
  exit 1
fi

cd "$ZYGOTE_DIR" || exit 1

echo "Establishing SOCKS proxy"
ssh -N -D 60000 -i ~/.ssh/id_ed25519_internal jrlogin08i &

if [ -f requirements.txt ]; then
  echo "Found requirements.txt, installing..."
  proxychains pip install -r requirements.txt
fi

proxychains /bin/bash -c "$ZYGOTE_EXEC"
