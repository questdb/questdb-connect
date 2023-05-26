#!/bin/sh


rm -rf dist
python3 -m build
twine upload -r questdb-connect dist/*
