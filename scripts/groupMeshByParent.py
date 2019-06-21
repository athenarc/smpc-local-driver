#!/usr/bin/python3

import argparse

from utils import read_json, write_json, get_children


def main():
    parser = argparse.ArgumentParser(
        description='Group terms by parent')
    parser.add_argument('mesh', help='File containing mesh terms with their childer. See catalogue.py --help.')
    parser.add_argument('output', help='Output file (JSON)')
    parser.add_argument('--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    mesh_terms = read_json(args.mesh)
    all_parents = set(v["parentCategoryId"] for k,v in mesh_terms.items())
    all_parents_dict = {}
    for parent in all_parents:
        all_parents_dict[parent] = get_children(parent, mesh_terms)
    write_json(args.output, all_parents_dict)

if __name__ == '__main__':
    main()