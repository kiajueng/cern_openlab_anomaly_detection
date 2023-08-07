import torch
import numpy as np
import pandas as pd
import copy
import json
import datahandler as dh
import model as my
import os

class early_stopping():
    def __init__(self,patience,mindelta):
        self.patience = patience
        self.mindelta = mindelta
        self.ref_loss = np.inf
        self.count = 0
        self.stop = False

    def __call__(self,val_loss):
        if (self.ref_loss - val_loss) < self.mindelta:
            self.count += 1
            if self.count >= self.patience:
                self.stop = True
        else:
            self.count = 0
            self.ref_loss = val_loss


class best_model():
    def __init__(self):
        self.checkpoint = {"loss":np.inf,
                      "model_state":None,
                      "optimizer_state":None,
                      "epoch":-1}

    def __call__(self,val_loss,model_state,epoch,optimizer_state):

        if self.checkpoint["loss"] > val_loss:
            self.checkpoint["optimizer_state"] = optimizer_state
            self.checkpoint["model_state"] = model_state
            self.checkpoint["loss"] = val_loss
            self.checkpoint["epoch"] = epoch
            
class run_training():

    def __init__(self,model,train_dataloader,val_dataloader):
        self.model = model.double()
        self.train_dataloader = train_dataloader
        self.val_dataloader = val_dataloader

    def __train(self,train_dataloader,optimizer,model,loss_fn,device):
        model_on_device = model.to(device)
        model_on_device.train()
        
        losses = []
        for x,y_true in train_dataloader:

            optimizer.zero_grad()
            x = x.to(device)
            y_true = y_true.to(device)
            y_pred = model_on_device(x)[:,-1]
            loss = loss_fn(y_pred, y_true)
            loss.backward()
            optimizer.step()

            losses.append(loss.cpu().detach().numpy())
        return np.mean(losses)

    def __validate(self,val_dataloader,model,loss_fn,device):

        model_on_device = model.to(device)
        model_on_device.eval()
        
        losses = []

        with torch.no_grad():
            for x,y_true in val_dataloader:
                y_true = y_true.to(device)
                x = x.to(device)
                y_pred = model_on_device(x)[:,-1]
                loss = loss_fn(y_pred,y_true)

                losses.append(loss.cpu().detach().numpy())
            
        return np.mean(losses)


    def __save(self,best_checkpoint,path):
        torch.save(best_checkpoint, path)

    
    def run(self,epochs,loss_fn,lr,optimizer,device,patience=None,mindelta=None):
        history = {"train":{}, "val":{}}

        if ((patience != None) & (mindelta != None)):
            earlystopping = early_stopping(patience,mindelta)

        bestmodel = best_model()
        
        for i in range(epochs):
            train_loss = self.__train(self.train_dataloader,optimizer,self.model,loss_fn,device)
            val_loss = self.__validate(self.val_dataloader,self.model,loss_fn,device)
            
            history["train"][f"{i}"] = train_loss 
            history["val"][f"{i}"] = val_loss
            
            earlystopping(val_loss)
            bestmodel(val_loss=val_loss,
                      model_state=copy.deepcopy(self.model.cpu()).state_dict(),
                      optimizer_state = copy.deepcopy(optimizer).state_dict(),
                      epoch=i)
            print(f"Epoch: {i}, Train_loss: {train_loss}, Val_loss: {val_loss}")
            if earlystopping.stop:
                break

        if not os.path.isdir(f"{os.environ.get('BASE_DIR')}/Model"):
            os.mkdir(f"{os.environ.get('BASE_DIR')}/Model")
        else:
            print("WARNING: Model directory already exist, history and checkpoint will be overwritten")

        with open(f"{os.environ.get('BASE_DIR')}/Model/history.json", "w") as file:
            json.dump(history, file)
        self.__save(bestmodel.checkpoint,f"{os.environ.get('BASE_DIR')}/Model/checkpoint.tar")

