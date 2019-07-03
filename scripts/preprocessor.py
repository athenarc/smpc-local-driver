#!/usr/bin/python3

import argparse
import json
from algorithms import (
    Processor,
    OneDimensionCategoricalHistogram,
    TwoDimensionCategoricalHistogram,
    TwoDimensionNumericalHistogram,
    OneDimensionNumericalHistogram
)


def preprocess(args):

    algorithms = {
        '1d_categorical_histogram': OneDimensionCategoricalHistogram,
        '2d_categorical_histogram': TwoDimensionCategoricalHistogram,
        '1d_numerical_histogram': OneDimensionNumericalHistogram,
        '2d_numerical_histogram': TwoDimensionNumericalHistogram
    }

    Algorithm = algorithms[args.algorithm]

    request = None

    if (args.request):
        request = json.loads(args.request)

    processor = Processor(Algorithm(args.computation, args.attributes, args.dataset, args.precision, request))
    processor.process()


def main():
    parser = argparse.ArgumentParser(
        description='SMPC local drive preprocessor')
    parser.add_argument(
        '-c',
        '--computation',
        required=True,
        type=str,
        help='The ID of the computation (required)')
    parser.add_argument(
        '-d',
        '--dataset',
        required=True,
        type=str,
        help='Dataset file (required)')
    parser.add_argument(
        '-a',
        '--attributes',
        nargs='*',
        help='List of space seperated attributes which will be included in the output file (default: all)')
    parser.add_argument(
        '-g',
        '--algorithm',
        default='1d_categorical_histogram',
        choices=[
            '2d_mixed_histogram',
            '1d_categorical_histogram',
            '1d_numerical_histogram',
            'secure_aggregation',
            '2d_numerical_histogram',
            '2d_categorical_histogram'],
        type=str,
        help='Computation algorithm (default: %(default)s)')
    parser.add_argument(
        '-p',
        '--precision',
        default=5,
        type=int,
        help='The number of decimal digits to be consider for floats (default: %(default)s)')
    parser.add_argument(
        '-r',
        '--request',
        type=str,
        help='The description of the computation request')

    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()

    preprocess(args)


if __name__ == '__main__':
    main()
