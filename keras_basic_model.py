# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import numpy as np

import keras
from keras.models import Model
from keras.layers import Dense,Activation,Input
from keras.callbacks import ModelCheckpoint

X = np.random.normal(0,1,(100,8))
Y = np.random.normal(0,1,(100,1))
X.shape


batch = 32
valX,valY = np.random.normal(0,1,(100,8)),np.random.normal(0,1,(100,1))
class LossHistory(keras.callbacks.Callback):
    def on_train_begin(self, logs={}):
        self.losses = []
        self.val_loss=[]
        self.weights= []

    def on_epoch_end(self, batch, logs={}):
        self.losses.append(logs.get('loss'))
        self.val_loss.append(logs.get('val_loss'))
        self.weights.append(self.model.get_weights())
        name = 'weights'+'_'+str(batch)+'.h5'
        self.model.save_weights(name) 


def keras_models(X,Y,kernel_init = 'random_uniform',output_activation = 'tanh',input_activation = 'relu',
                validation_data = [valX,valY]):
    losses = LossHistory()
    ip = Input(batch_shape=(batch,X.shape[1]))
    layer1 = Dense(32, kernel_initializer=kernel_init)(ip)
    layer2 = Activation(input_activation)(layer1)
    out = Dense(Y.shape[1],activation = output_activation)(layer2)
    model = Model(inputs = ip,output = out)
    model.compile(optimizer='adam',loss = 'mean_squared_error')
    filepath="weights-improvement-{epoch:02d}-{val_acc:.2f}.hdf5"
    checkpoint = ModelCheckpoint(filepath, monitor='val_loss', verbose=1, save_best_only=True)
    callbacks_list = [losses]#,checkpoint]
    model.fit(X,Y,validation_data=validation_data,batch_size=batch,epochs=100,callbacks=callbacks_list,verbose=1)
    return model,losses


model = keras_models(X,Y,kernel_init = 'random_uniform',output_activation = 'tanh',input_activation = 'relu',
                validation_data = [valX,valY])