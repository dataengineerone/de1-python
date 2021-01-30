#!/bin/bash
CUR_DIR=$(cd `dirname $0`; pwd)
cd $CUR_DIR
rm -rf dist
source $CUR_DIR/venv/bin/activate
python setup.py sdist bdist_wheel
python -m twine upload --repository pypi dist/*

