#!/usr/bin/python3

import json
import argparse


def read_file(file):
    with open(file) as f:
        return json.load(f)


def write_file(file, out):
    with open(file, 'w') as f:
        json.dump(out, f, indent=4)


def process(args, callback):
    catalogue = None
    out = []

    catalogue = read_file(args.catalogue)

    for entry in catalogue:
        callback(entry, out)

    write_file(args.output, out)


def normal(args):
    process(args, lambda entry, out: out.append({entry['id']: entry}))


def inverse(args):
    process(args, lambda entry, out: out.append({entry['name']: entry}))


def group(args):
    catalogue = None
    normalized_catalogue = {}
    out = []

    catalogue = read_file(args.catalogue)

    for entry in catalogue:
        normalized_catalogue[entry['id']] = entry
        normalized_catalogue[entry['id']]['children'] = []

    for id in normalized_catalogue:
        parent_id = normalized_catalogue[id]['parentCategoryId']
        if parent_id in normalized_catalogue:
            normalized_catalogue[parent_id]['children'].append({key: normalized_catalogue[id][key] for key in normalized_catalogue[id] if key != 'children'})
        out.append(normalized_catalogue[id])

    write_file(args.output, out)


def main():
    parser = argparse.ArgumentParser(
        description='SMPC catalogue processor')
    parser.add_argument('catalogue', nargs='?', help='Catalogue file')
    parser.add_argument('output', nargs='?', help='Output file')
    parser.add_argument(
        '-i',
        '--inverse',
        action='store_true',
        help='Create a file where the key of each entry is the name of the term')
    parser.add_argument(
        '-n',
        '--normal',
        action='store_true',
        help='Create a file where the key of each entry is the id mesh term')
    parser.add_argument(
        '-g',
        '--group',
        action='store_true',
        help='Create a file where the mesh terms are grouped')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()

    if (args.inverse or args.group or args.normal) and (not args.catalogue or not args.output):
        parser.error("<catalogue> <output> required with --inverse, --normal, and --group")

    if(args.inverse):
        inverse(args)
    elif(args.group):
        group(args)
    else:
        normal(args)


if __name__ == '__main__':
    main()
