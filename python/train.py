
import torch
import torch.nn as nn
import datahandler as dh
import run
import model as m
import os
from torch.utils.data import DataLoader, Dataset, Subset
import torchvision.datasets as datasets
import numpy as np

#Initialise device 
os.environ['CUDA_LAUNCH_BLOCKING'] = "1"
use_cuda = True
use_cuda = False if not use_cuda else torch.cuda.is_available()
device = torch.device('cuda:0' if use_cuda else 'cpu')
torch.cuda.get_device_name(device) if use_cuda else 'cpu'
print('Using device', device)

#Initialise model
model = m.RNN(input_dim = 1, hidden_dim = 16, layer_dim = 3, output_dim = 1)
print("Model Initialized")

#Initialise optimizer + loss function
lr = 0.001
optimizer = torch.optim.Adam(model.parameters(),lr=lr)
loss = nn.L1Loss()
print("Learning rate, optimizer and loss initialized")

#Initialize data to dataloader
data = dh.loader(2023,5,21,2023,5,21,"RunParams_ROD_CRATE_B2_ROD_B2_S7_L0_B07_S1_C7_M6C_FMTOccupancy").data
train_x,train_y,test_x,test_y = dh.TS_window(data,1000,0.8).windows()
test_dataset = dh.TS_Dataset(test_x,test_y)
train_dataset = dh.TS_Dataset(train_x,train_y)
test_loader = DataLoader(test_dataset,shuffle=False,batch_size=1000)
train_loader = DataLoader(train_dataset,shuffle=False,batch_size=1000)

print("Dataloader initialized")

#Initialize the run_training class from run.py
runner = run.run_training(model,train_loader,test_loader)
print("Model now running")
runner.run(epochs=20,loss_fn=loss,lr=lr,optimizer=optimizer,device=device,patience=3,mindelta=1)

