#!/bin/bash
git submodule init
git submodule update
yarn install

mkdir -p datasets
mkdir -p requests

cd scripts
pip3 install -r requirements.txt
