#!/usr/bin/python3

import json
import argparse
import xml.etree.ElementTree as ET
import re


def generate(dataset, output):
    xml = ET.parse(dataset)
    root = xml.getroot()

    string = '{(.*?)}'
    prefix = re.search(string, root.tag)[0]
    attributes = set()

    for tname in root.findall('.//' + prefix + 'TypeName'):
        attributes.add(tname.text)

    with open(output, 'w') as f:
        json.dump(list(attributes), f, indent=4)


def main():
    parser = argparse.ArgumentParser(
        description='SMPC attribute generator')
    parser.add_argument(
        '-d',
        '--dataset',
        required=True,
        type=str,
        help='Dataset file (required)')
    parser.add_argument(
        '-o',
        '--output',
        required=True,
        type=str,
        help='Output mapping file (required)')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()
    generate(args.dataset, args.output)


if __name__ == '__main__':
    main()
