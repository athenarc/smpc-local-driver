#!/usr/bin/python3

import argparse
import xml.etree.ElementTree as ET
import re

from utils import read_json, write_json


def map_mesh_terms(args):
    global_map = {}

    mesh_terms = read_json(args.mesh)

    for term in mesh_terms:
        global_map[term['id']] = {}
        counter = 0
        for child in term['children']:
            global_map[term['id']][child['id']] = counter
            counter += 1

    write_json(args.output, global_map)


def main():
    parser = argparse.ArgumentParser(
        description='SMPC global mapping generator')
    parser.add_argument('mesh', help='File containing mesh terms with their childer. See catalogue.py --help.')
    parser.add_argument('output', help='Output file (JSON)')
    parser.add_argument('--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    map_mesh_terms(args)


if __name__ == '__main__':
    main()
