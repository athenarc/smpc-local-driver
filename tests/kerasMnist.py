from __future__ import print_function
import keras
from keras.datasets import mnist
from keras.models import Model, save_model, load_model
import numpy as np
from numpy.random import randint as rand
import keras.layers as l
from keras import backend as K
import matplotlib.pyplot as plt
from tqdm import tqdm
from lat1 import LAT1
import gc

def save_weights(model,filename):
  # To save topology uncomment
  # # serialize model to JSON
  # model_json = model.to_json()
  # with open(filename + "_topology", "w") as json_file:
  #     json_file.write(model_json)
  #######
  # serialize weights to HDF5
  model.save_weights(filename +".h5")
  # print("Saved model to disk")

    # later...
def load_weights_from_file(model,filename):
  # # load json and create model
  # json_file = open('model.json', 'r')
  # loaded_model_json = json_file.read()
  # json_file.close()
  # loaded_model = model_from_json(loaded_model_json)
  # load weights into new model
  model.load_weights(filename+".h5")
  # print("Loaded model from disk")


def get_weight_grad(model, inputs, outputs):
  """ Gets gradient of model for given inputs and outputs for all weights"""
  grads = model.optimizer.get_gradients(model.total_loss, model.trainable_weights)
  symb_inputs = (model._feed_inputs + model._feed_targets + model._feed_sample_weights)
  f = K.function(symb_inputs, grads)
  x, y, sample_weight = model._standardize_user_data(inputs, outputs)
  output_grad = f(x + y + sample_weight)
  return output_grad

batch_size = 1
num_classes = 10

# input image dimensions
img_rows, img_cols = 28, 28

# the data, split between train and test sets
(x_train, y_train), (x_test, y_test) = mnist.load_data()

if K.image_data_format() == 'channels_first':
  x_train = x_train.reshape(x_train.shape[0], 1, img_rows, img_cols)
  x_test = x_test.reshape(x_test.shape[0], 1, img_rows, img_cols)
  input_shape = (1, img_rows, img_cols)
else:
  x_train = x_train.reshape(x_train.shape[0], img_rows, img_cols, 1)
  x_test = x_test.reshape(x_test.shape[0], img_rows, img_cols, 1)
  input_shape = (img_rows, img_cols, 1)

x_train = x_train.astype('float32')
x_test = x_test.astype('float32')
x_train /= 255
x_test /= 255
print('x_train shape:', x_train.shape)
print(x_train.shape[0], 'train samples')
print(x_test.shape[0], 'test samples')

# convert class vectors to binary class matrices
y_train = keras.utils.to_categorical(y_train, num_classes)
y_test = keras.utils.to_categorical(y_test, num_classes)
x_train_trans = x_train.reshape((60000,28,28))
# plt.imshow(x_train_trans[0])
# plt.show()
def define_simple_model():
  input_layer = l.Input((28,28,1))
  flat = l.Flatten()(input_layer)
  dense1 = l.Dense(128,activation='relu')(flat)
  dense2 = l.Dense(64,activation='relu')(dense1)
  output_layer = l.Dense(10, activation='softmax')(dense2)

  model_simple = Model(input_layer, output_layer)
  # model_simple.summary()
  model_simple.compile(loss=keras.losses.categorical_crossentropy,
                optimizer=keras.optimizers.SGD(lr=0.01),
                metrics=['accuracy'])
  return model_simple


model_simple = define_simple_model()
# load_weights_from_file(model_simple, "test_noisy+_weights")

print(model_simple.evaluate(x_test,y_test))
for ITER in tqdm(range(2500)):

  if (ITER % 100 == 0) and (ITER !=0):
    print(model_simple.evaluate(x_test,y_test))

  if (ITER % 10 == 0) and (ITER !=0):
    save_weights(model_simple,"test_noisy+_weights")
    K.clear_session()
    gc.collect()
    model_simple = define_simple_model()
    load_weights_from_file(model_simple, "test_noisy+_weights")

  idx = rand(0,x_train.shape[0],32)
  x_sample = x_train[idx]
  y_sample = y_train[idx]


  # model_simple.train_on_batch(x_sample,y_sample)
  weights = np.array(model_simple.get_weights())
  gradients = np.array(get_weight_grad(model_simple,x_sample,y_sample))
  # print(gradients[0].shape)
  # np.save("test_gradients/o1", gradients)
  #new_weights = list(weights - 0.0075 * (gradients))
  new_weights = list(weights - 0.1 * LAT1(gradients,epsilon=10,local_batch_size=32,batch_size=32, gamma=0.001, subsample_rate=0.01))
  model_simple.set_weights(new_weights)

save_weights(model_simple,"test_noisy+_weights")
print("-----------------------")

