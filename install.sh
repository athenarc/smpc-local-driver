#!/bin/bash
git submodule init
git submodule update
yarn install

mkdir -p datasets
mkdir -p requests
