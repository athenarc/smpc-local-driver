#!/usr/bin/python3

import argparse

from utils import read_json, write_json


def parse_unique_id(mesh_terms):
    counter = 0
    global_map = {}

    for term in mesh_terms:
        global_map[term['code']] = counter
        counter += 1

    return global_map


def parse_tree_numbers(mesh_terms):
    counter = 0
    global_map = {}

    for term in mesh_terms:
        for id in term['ids']:
            if id not in global_map:
                global_map[id] = counter
                counter += 1

    return global_map


def map_mesh_terms(args):
    global_map = {}

    mesh_terms = read_json(args.mesh)
    if args.unique:
        global_map = parse_unique_id(mesh_terms)
    else:
        global_map = parse_tree_numbers(mesh_terms)

    write_json(args.output, global_map)


def main():
    parser = argparse.ArgumentParser(
        description='SMPC global mapping generator')
    parser.add_argument('mesh', help='File containing mesh terms in XML format as defined in ftp://nlmpubs.nlm.nih.gov/online/mesh')
    parser.add_argument('output', help='Output file (JSON)')
    parser.add_argument(
        '-u',
        '--unique',
        action='store_true',
        help='Create a mapping from Mesh Unique ID')
    parser.add_argument('--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    map_mesh_terms(args)


if __name__ == '__main__':
    main()
