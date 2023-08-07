import pandas as pd
import glob
import os
from torch.utils.data import DataLoader, Dataset, Subset
import torchvision.datasets as datasets
import calendar
import numpy as np

class TS_Dataset(Dataset):
    def __init__(self,x,y):
        self.x = x
        self.y = y

    def __len__(self):
        return len(self.x)

    def __getitem__(self,index):
        return self.x[index], self.y[index] 

class loader():
    #Loads the data and concatenates it into one large DF
    
    def __init__(self,s_year,s_month,s_day,e_year,e_month,e_day,pattern):
        self.s_year = s_year
        self.s_month = s_month
        self.s_day = s_day
        self.e_year = e_year
        self.e_month = e_month
        self.e_day = e_day
        self.pattern = pattern
        self.data = self.load()

    def load_day(self,year,month,day,pattern):
        dfs = []
        for files in glob.glob(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{year}/{month}/{day}/{pattern}*"):
            dfs.append(pd.read_hdf(files).set_index("Date_Time"))
        if len(dfs) == 0:
            raise ValueError("No File read out")
        data = pd.concat(dfs,axis=1)
        return data

    def month_numeric(val, option = "to_numeric"):
        month_dict = {1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun", 7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dez"}
        month_dict_rev = {"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11, "Dez":12}
    
        if(option == "to_numeric"):
            return month_dict_rev[val]
        if(option == "to_month"):
            return month_dict[val]
        raise OptionError("Option has to be either 'to_numeric' or 'to_month'")

    def load(self):
        to_be_concat = []
        while(self.s_year <= self.e_year):
            while(self.s_month <= 12):
                while(self.s_day <= calendar.monthrange(self.s_year, self.s_month)[1]):
                    if (self.s_year == self.e_year and self.s_month == self.s_month and self.s_day > self.e_day):
                        break
                    to_be_concat.append(self.load_day(self.s_year,
                                                      "May",
                                                      self.s_day,
                                                      self.pattern))
                    self.s_day += 1
                self.s_month += 1
                if (self.s_year == self.e_year and self.s_month > self.e_month):
                    break
            self.s_year += 1
            self.s_month = 1

        data = pd.concat(to_be_concat)    
        nunique = data.nunique()
        cols_to_drop = nunique[nunique == 1].index
        data.drop(cols_to_drop, axis=1, inplace = True)

        return(data)

class TS_window():

    def __init__(self,data,window,split):
        self.data = data
        self.window = window
        self.split = int(split*data.shape[0])
        
    def windows(self):
        train_x = []
        train_y = []
        test_x = []
        test_y = []
        for i in range(self.split - self.window):
            for j in range(self.data.shape[1]):
                train_x.append(self.data.values[i:i+self.window,j:j+1])     #j:j+1 used so it has right dimension
                train_y.append(self.data.values[i+self.window,j:j+1])

        for i in range(self.data.shape[0] - self.split - self.window):
            for j in range(self.data.shape[1]):
                test_x.append(self.data.values[self.split+i:self.split+i+self.window,j:j+1])
                test_y.append(self.data.values[i+self.window+self.split,j:j+1])
        return (train_x,train_y,test_x,test_y)
