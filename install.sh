#!/bin/bash
git submodule init
git submodule update
yarn install

mkdir -p certs
mkdir -p data 
mkdir -p scale/certs
