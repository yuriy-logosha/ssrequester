#! /bin/bash
pip install -r requirements.txt

pm2 stop ssrequester

pm2 start src/ssrequester.py --interpreter=python3