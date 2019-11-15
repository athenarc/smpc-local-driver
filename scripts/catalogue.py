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
    out = {}

    catalogue = read_file(args.catalogue)

    for c in catalogue:
        callback(catalogue[c], out)

    write_file(args.output, out)


def normal(args):
    process(args, lambda entry, out: out.update({entry['id']: entry}))


def inverse(args):
    process(args, lambda entry, out: out.update({entry['name']: entry}))


def code(args):
    process(args, lambda entry, out: out.update({entry['code']: entry}))


def group(args):
    catalogue = None
    normalized_catalogue = {}
    out = []

    catalogue = read_file(args.catalogue)

    for c in catalogue:
        normalized_catalogue[catalogue[c]['id']] = catalogue[c]
        normalized_catalogue[catalogue[c]['id']]['children'] = []

    for id in normalized_catalogue:
        parent_id = normalized_catalogue[id]['parentCategoryId']
        if parent_id in normalized_catalogue:
            normalized_catalogue[parent_id]['children'].append({key: normalized_catalogue[id][key] for key in normalized_catalogue[id] if key != 'children'})
        out.append(normalized_catalogue[id])

    write_file(args.output, out)


def keywords(args):
    keywords = read_file(args.catalogue)
    mesh = read_file('../smpc-global/meshTermsInversed.json')
    out = []

    for k in keywords:
        if k['label'] in mesh:
            out.append(mesh[k['label']])

    write_file(args.output, out)


def main():
    parser = argparse.ArgumentParser(
        description=(
            'An SMPC catalogue processor that process mesh terms and output various structures. '
            'The input file must be as exported from mesh.py with flag --flatten. \n\n'
            'Example: \n'
            '1) Download an xml mesh file from ftp://nlmpubs.nlm.nih.gov/online/mesh.\n'
            '2) Run mesh.py mesh.xml output.json. \n'
            '3) Run mesh.py output.json flatten.json --flatten. \n'
            '4) Run catalogue.py flatten.json normal.json --normal. \n'
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('catalogue', help='Catalogue file')
    parser.add_argument('output', help='Output file')
    parser.add_argument(
        '-i',
        '--inverse',
        action='store_true',
        help='Create a file where the key of each entry is the name of the term.')
    parser.add_argument(
        '-n',
        '--normal',
        action='store_true',
        help='Create a file where the key of each entry is the id mesh term.')
    parser.add_argument(
        '-c',
        '--code',
        action='store_true',
        help='Create a file where the key of each entry is the code mesh term.')
    parser.add_argument(
        '-g',
        '--group',
        action='store_true',
        help='Create a file where each mesh term has its children.')
    parser.add_argument(
        '-k',
        '--keywords',
        action='store_true',
        help='Create a file with keywords as mesh terms. File structure must be as returned from the catalogue API.')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()

    if (args.inverse or args.group or args.normal or args.code) and (not args.catalogue or not args.output):
        parser.error("<catalogue> <output> required with --inverse, --normal, --code, and --group")

    if(args.inverse):
        inverse(args)
    elif(args.code):
        code(args)
    elif(args.group):
        group(args)
    elif(args.keywords):
        keywords(args)
    else:
        normal(args)


if __name__ == '__main__':
    main()
