#!/bin/bash
CUR_DIR=$(cd `dirname $0`; pwd)
cd $CUR_DIR
rm -rf dist
python setup.py sdist bdist_wheel
python -m twine upload --repository pypi dist/*
