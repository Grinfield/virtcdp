#!/usr/bin/bash

workdir=$(cd $(dirname $(dirname $0)); pwd)
python3 -m venv ${workdir}/.venv --system-site-packages
source ${workdir}/.venv/bin/activate
pip3 install -r ${workdir}/requirements.txt
if [ $? -ne 0 ]; then
    echo 'pip failed'; exit
fi

python3 ${workdir}/virtcdp/cmd/api.py --config-file ${workdir}/etc/virtcdp.conf & 2>&1 /dev/null
if [ $? -ne 0 ]; then
    echo 'Api service failed!'; exit
fi

python3 ${workdir}/virtcdp/cmd/agent.py --config-file ${workdir}/etc/virtcdp.conf & 2>&1 /dev/null
if [ $? -ne 0 ]; then
    echo 'Agent service failed!'; exit
fi
