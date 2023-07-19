import numpy as np
import pandas as pd
import glob
import os
import argparse
import calendar
from datetime import datetime,timedelta

parser = argparse.ArgumentParser() 
parser.add_argument('-sd', '--start_day', type = int, required=True)
parser.add_argument('-sm', '--start_month', type = int, required=True)
parser.add_argument('-sy', '--start_year', type = int, required=True)
parser.add_argument('-ed', '--end_day', type = int, required=True)
parser.add_argument('-em', '--end_month', type = int, required=True)
parser.add_argument('-ey', '--end_year', type = int, required=True)
parser.add_argument('-p', '--patterns', action = 'append', required=True, help='Pattern to load from files')
parser.add_argument('-f', '--data_frequency', type = str, help='data publishing frequency', required=True)
args = parser.parse_args()

def month_numeric_switch(val, option = "to_numeric"):
    month_dict = {1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun", 7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dez"}
    month_dict_rev = {"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11, "Dez":12}
    
    if(option == "to_numeric"):
        return month_dict_rev[val]
    if(option == "to_month"):
        return month_dict[val]
    raise OptionError("Option has to be either 'to_numeric' or 'to_month'")
    

class preprocessor_pbeast:

    def __init__(self, day, month, year, patterns=[],data_freq="6S",how_interpolate="linear"):
        self.patterns = patterns
        self.day = day
        self.month = month
        self.year = year
        self.data_freq = data_freq
        self.how_interpolate = how_interpolate
        self.__run()

    def __read_csv(self):
        dfs = []
        for pattern in self.patterns:
            for files in glob.glob(f"{os.environ.get('BASE_DIR')}/Data/{self.year}/{self.month}/{self.day}/{pattern}"):
                dfs.append(pd.read_csv(files, delimiter=","))
        return dfs
    
    def __interval_to_points(self,dfs):
        for i,df in enumerate(dfs):
            df.loc[~df["Date_Time"].str.contains(" - "), "Date_Time"] = pd.to_datetime(df.loc[~df["Date_Time"].str.contains(" - "),"Date_Time"], format='%Y-%B-%d %H:%M:%S.%f').astype(str)
            from_to_rows = df.loc[df['Date_Time'].str.contains(" - ")].index     #Find rows where datetime is given as interval
            for index in reversed(from_to_rows):     #Reverse so index does not change
                tmp_start = df.iloc[:index].copy()
                tmp_end = df.iloc[index+1:].copy()

                start = pd.to_datetime(df.iloc[index]["Date_Time"].split(" - ")[0], format='%Y-%B-%d %H:%M:%S.%f')
                end = pd.to_datetime(df.iloc[index]["Date_Time"].split(" - ")[1], format='%Y-%B-%d %H:%M:%S.%f')

                datetimeindex = pd.date_range(start,end,freq=self.data_freq,inclusive = "left")
                to_be_appended = pd.DataFrame({df.columns[0]:datetimeindex, 
                                               df.columns[1]:np.ones(len(datetimeindex))*df.iloc[index][df.columns[1]]})

                df = pd.concat([tmp_start,to_be_appended,tmp_end], ignore_index = True)
            df["Date_Time"] = pd.to_datetime(df["Date_Time"])
            dfs[i] = df
    
    def __rounding(self,df):
            df["Date_Time"] = df["Date_Time"].dt.round("S")
            start_day = datetime(self.year, month_numeric_switch(self.month, option = "to_numeric"), self.day)
            end_day = start_day + timedelta(days=1)
            df = df[(df["Date_Time"] >= start_day) & (df["Date_Time"] < end_day)]

    def __save(self,df):
        name = df.columns[1].replace(".","_").replace("/","_")
        df.to_hdf(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}/{self.day}/{name}.h5", key=f"{name}", mode="w")

    def __smooth_save(self,dfs):
        for i in range(len(dfs)):
            self.__rounding(dfs[i])
            dfs[i] = dfs[i].set_index("Date_Time").resample(self.data_freq).ffill()
            dfs[i].reset_index(inplace=True)
            
            start_day = datetime(self.year, month_numeric_switch(self.month, option = "to_numeric"), self.day)
            end_day = start_day + timedelta(days=1)
            dfs[i] = dfs[i][(dfs[i]["Date_Time"] >= start_day) & (dfs[i]["Date_Time"] < end_day)] #Delete all rows whose day is != the given day
            self.__save(dfs[i])
            
    def __run(self):
        initial_dfs = self.__read_csv()

        if not os.path.isdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data"):
            os.mkdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data")

        if not os.path.isdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}"):
            os.mkdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}")

        if not os.path.isdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}"):
            os.mkdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}")

        if not os.path.isdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}/{self.day}"):
            os.mkdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}/{self.day}")

        if initial_dfs:
            self.__interval_to_points(initial_dfs)
            self.__smooth_save(initial_dfs)
        return initial_dfs

cleaned_data = pd.DataFrame()
while(args.start_year <= args.end_year):
    while(args.start_month <= 12):
        while(args.start_day <= calendar.monthrange(args.start_year, args.start_month)[1]):
            if (args.start_year == args.end_year and args.start_month == args.start_month and args.start_day > args.end_day):
                break
            preprocessor_pbeast(day=args.start_day,
                                month=month_numeric_switch(args.start_month, option = "to_month"),
                                year=args.start_year,
                                patterns=args.patterns,
                                data_freq=args.data_frequency)  
            args.start_day += 1
        args.start_month += 1
        if (args.start_year == args.end_year and args.start_month > args.end_month):
            break
    args.start_year += 1
    args.start_month = 1
    
            
