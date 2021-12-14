#!/usr/bin/bash

workdir=$(cd $(dirname $0); pwd)
source .venv/bin/activate
pip3 install -r requirements.txt

python3 virtcdp/cmd/api.py --config-file etc/virtcdp.conf & 2>&1 /dev/null
python3 virtcdp/cmd/agent.py --config-file etc/virtcdp.conf & 2>&1 /dev/null
