#!/usr/bin/python3

import argparse
import requests
import re
from tqdm import tqdm
import xml.etree.ElementTree as ET

from utils import read_json, write_json


def process(args, callback):
    catalogue = None
    out = {}

    catalogue = read_json(args.catalogue)

    for entry in catalogue:
        callback(entry, out)

    write_json(args.output, out)


def normal(args):
    process(args, lambda entry, out: out.update({entry['id']: entry}))


def inverse(args):
    process(args, lambda entry, out: out.update({entry['name']: entry}))


def code(args):
    process(args, lambda entry, out: out.update({entry['code']: entry}))


def download(args):
    mesh_codes = read_json('../smpc-global/meshTermsByCode.json')
    xml = ET.parse(args.catalogue)
    root = xml.getroot()

    string = '{(.*?)}'
    prefix = re.search(string, root.tag)[0]
    attributes = set()
    values = set()
    mesh_terms = []

    for tname in root.findall('.//' + prefix + 'ClinicalVariables'):
        attribute = tname.find(prefix + 'TypeName').text
        value = tname.find(prefix + 'Value').text
        attributes.add(attribute)
        values.add(value)

    for attr in tqdm(attributes):
        data = {
            'term': attr,
            'terminology': 'text',
            'result_format': 'json'
        }

        response = requests.post(args.url[0].strip('\t\n\r'), data=data)
        terms = response.json()

        tmp = {'attribute': attr, 'terms': []}
        for t in terms:
            if t['mesh_code'] in mesh_codes:
                tmp['terms'].append(mesh_codes[t['mesh_code']])
            else:
                tmp['terms'].append(t)

        mesh_terms.append(tmp)

    write_json(args.output, mesh_terms)


def group(args):
    catalogue = None
    normalized_catalogue = {}
    out = []

    catalogue = read_json(args.catalogue)

    for entry in catalogue:
        normalized_catalogue[entry['id']] = entry
        normalized_catalogue[entry['id']]['children'] = []

    for id in normalized_catalogue:
        parent_id = normalized_catalogue[id]['parentCategoryId']
        if parent_id in normalized_catalogue:
            normalized_catalogue[parent_id]['children'].append({key: normalized_catalogue[id][key] for key in normalized_catalogue[id] if key != 'children'})
        out.append(normalized_catalogue[id])

    write_json(args.output, out)


def main():
    parser = argparse.ArgumentParser(
        description='SMPC catalogue processor')
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
        help='Create a file where each mesh term has its childer.')
    parser.add_argument(
        '-d',
        '--download',
        nargs=1,
        dest='url',
        help='Create a file where each attribute and value of the dataset is mapped to a mesh term by quering the catalogue')
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
    elif(args.url):
        download(args)
    else:
        normal(args)


if __name__ == '__main__':
    main()
