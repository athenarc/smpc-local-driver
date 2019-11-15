from numpy.random import laplace
import numpy as np
import argparse
import os 

def LAT(grads, data_size=60000, local_batch_size=1,batch_size=1, gamma=1, epsilon=0.1, subsample_rate=0.05):
    aggregate_grads = (1.0*local_batch_size/batch_size) * grads
    d = aggregate_grads.size
    ratio = data_size/batch_size
    denominator = np.log(ratio * (np.exp(epsilon) - 1) + 1)
    lambda_ = 2*gamma*d*subsample_rate/denominator
    for i in range(aggregate_grads.shape[0]):
        aggregate_grads[i] = aggregate_grads[i].clip(-gamma, gamma)

    return aggregate_grads + laplace(0,lambda_,aggregate_grads.shape)

parser = argparse.ArgumentParser(description='Inject noise to a given gradient file.')

parser.add_argument('-gr', '--grads', metavar='grads', type=str, default="outputs/grads.h5",
                    help='Black box output file (eg. outputs/grads.h5)')
parser.add_argument('-o', '--output_dir', metavar='output_dir', type=str, default="private_outputs/",
                    help='Directory where output will be stored (eg. private_outputs/)')
parser.add_argument('-ds', '--data_size', metavar='data_size', type=int, default=60000,
                    help='Dataset Size')
parser.add_argument('-lbs', '--local_batch_size', metavar='local_batch_size', type=int, default=32,
                    help='Local batch size')
parser.add_argument('--bs', metavar='batch_size', type=int, default=32,
                    help='Global batch_size')
parser.add_argument('--gamma', metavar='gamma', type=float, default=0.1,
                    help='Truncation Parameter')
parser.add_argument('-eps', '--epsilon', metavar='epsilon', type=float, default=0.1,
                    help='Privacy Budget')
parser.add_argument('-ssr', '--subsample_rate', metavar='subsample_rate', type=float, default=0.05,
                    help='Subsample rate')
args, unknown = parser.parse_known_args()

assert os.path.exists(args.grads), 'Enter a valid "grads" directory'
assert os.path.exists(args.output_dir), 'Enter a valid "output" directory'
grads = args.grads

np.save(args.output_dir + "test", LAT(grads, args.data_size, args.local_batch_size, args.batch_size, args.gamma, args.epsilon, args.subsample_rate))


