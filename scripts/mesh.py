#!/usr/bin/python3

import json
import argparse
import xml.etree.ElementTree as ET
import os


def read_file(file):
    with open(file) as f:
        return json.load(f)


def write_file(file, out):
    with open(file, 'w') as f:
        json.dump(out, f, indent=4)


def process_json(args):
    terms = read_file(args.mesh)
    out = {}

    for t in terms:
        for id in t['ids']:
            out[id] = {'id': id, 'code': t['code'], 'parentCategoryId': id[:-4], 'name': t['name']}

    write_file(args.output, out)


def process_xml(args):
    tree = ET.parse(args.mesh)
    root = tree.getroot()
    mesh_terms = []

    for xml_record in root:
        uid = xml_record.find('DescriptorUI').text
        name = xml_record.find('DescriptorName').find('String').text
        terms = xml_record.find('TreeNumberList')
        rec = {'code': uid, 'name': name, 'ids': []}

        if not (terms):
            continue

        for term in list(terms):
            rec['ids'].append(term.text)

        mesh_terms.append(rec)

    write_file(args.output, mesh_terms)


def process(args):
    extension = os.path.splitext(args.mesh)[1]

    if(extension == '.xml'):
        process_xml(args)
    elif(extension == '.json'):
        process_json(args)
    else:
        print('Unsupported file')


def main():
    parser = argparse.ArgumentParser(
        description='Covert an xml file containing mesh terms as in NIH to json')
    parser.add_argument('mesh', nargs='?', help='File containing mesh terms in XML format as defined in ftp://nlmpubs.nlm.nih.gov/online/mesh or a json to be flatten')
    parser.add_argument('output', nargs='?', help='Output file (JSON)')
    parser.add_argument(
        '-f',
        '--flatten',
        action='store_true',
        help='Create a file where each mesh term has its children.')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()
    process(args)


if __name__ == '__main__':
    main()
