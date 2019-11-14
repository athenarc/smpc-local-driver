from numpy.random import laplace
import numpy as np
import matplotlib.pyplot as plt
import h5py

def LAT1(grads, data_size=60000, local_batch_size=1,batch_size=1, gamma=1, epsilon=0.1, subsample_rate=0.05):
  aggregate_grads = (1.0*local_batch_size/batch_size) * grads
  d = np.sum([aggregate_grads[i].size for i in range(aggregate_grads.shape[0])])
  ratio = data_size/batch_size
  denominator = np.log(ratio * (np.exp(epsilon) - 1) + 1)
  lambda_ = 2*gamma*d*subsample_rate/denominator
  zeros = []
  for i in range(aggregate_grads.shape[0]):
    aggregate_grads[i] = aggregate_grads[i].clip(-gamma, gamma)
    aggregate_grads[i] += laplace(0,lambda_,aggregate_grads[i].shape)
    top = int(subsample_rate * aggregate_grads[i].size)
    if top == 0: top =1
    top_n_idxs = np.argpartition(aggregate_grads[i].reshape(-1), -top)[-top:]
    a = np.zeros(aggregate_grads[i].shape)
    a.reshape(-1)[top_n_idxs] = aggregate_grads[i].reshape(-1)[top_n_idxs]
    zeros.append(a)
  # to_return.reshape(-1)[top_n_idxs] = aggregate_grads.reshape(-1)[top_n_idxs]
  return np.array(zeros)

if __name__=="__main__":
  grads = np.load("test_gradients/o1.npy")
  aq = LAT1(grads)
  np.save("test_private_gradients/po1.npy", aq)