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

    def __init__(self, day, month, year, patterns=[],data_freq="6S"):
        self.patterns = patterns
        self.day = day
        self.month = month
        self.year = year
        self.data_freq = data_freq
        self.__run()

    def __read_csv(self):
        dfs = []
        for pattern in self.patterns:
            for files in glob.glob(f"{os.environ.get('BASE_DIR')}/Data/{self.year}/{self.month}/{self.day}/{pattern}"):
                dfs.append(pd.read_hdf(files))
        if (not os.path.isfile(f"{os.environ.get('BASE_DIR')}/Data/{self.year}/{self.month}/{self.day}/DF_HLTSV_Events.h5")) or (len(dfs) == 0):
            print(f"No Trigger rates for {self.day}.{self.month}.{self.year}")
            return None,None
        L1 = pd.read_hdf(f"{os.environ.get('BASE_DIR')}/Data/{self.year}/{self.month}/{self.day}/DF_HLTSV_Events.h5")
        return L1,dfs


    def __resample(self,df,freq):
        #Treat the intervals
        df_new = df.copy()

        if len(df.loc[df['Date_Time'].str.contains(" - ")].index) > 0: 
            last_interval_idx = df.loc[df['Date_Time'].str.contains(" - ")].index[-1]
            last_idx = df.index[-1]
        if last_interval_idx == last_idx:
            interval = df.iloc[[last_interval_idx]].copy().rename(index={last_interval_idx:(last_interval_idx +1)})
            interval["Date_Time"] = interval["Date_Time"].str.split(" - ").str[1]
            df_new = pd.concat([df,interval])
        df_new['Date_Time'] = df_new['Date_Time'].str.split(' - ').str[0]
        df_new['Date_Time'] = pd.to_datetime(df_new['Date_Time'], format='%Y-%b-%d %H:%M:%S.%f').dt.round('1s')                                                                                          
        # first resampling to fill periods with " - " with actual values                                                                                                                         
        df_new = df_new.set_index('Date_Time')                                                                                                                                                           
        df_new = df_new.resample(freq).ffill().bfill()
        df_new.reset_index(inplace=True)
        return df_new
    
    def __join(self,dfs):
        for i in range(len(dfs)):
            dfs[i]=dfs[i].set_index("Date_Time")
        joined_df = pd.concat(dfs,axis=1,join="outer").sort_index().reset_index()
        return joined_df
        
    def __drop_L1(self,df,L1):

        # which events to remove                                                                                                                                                                 
        condition = L1['DF.HLTSV.Events'] <= 1049                                                                                                                                                 
        drop_times = L1.loc[condition, "Date_Time"]

        # remove given points in time                                                                                                                                                            
        rows_to_remove_indices = []                                                                                                                                                              
        for given_timestamp in drop_times:                                                                                                                                                          
            matching_rows = df.loc[(df['Date_Time'] >= given_timestamp - pd.Timedelta(seconds=10)) &                                                                                           
                                    (df['Date_Time'] <= given_timestamp + pd.Timedelta(seconds=10))]                                                                                         
            rows_to_remove_indices.extend(matching_rows.index)                                                                                                                                   
        df = df.drop(rows_to_remove_indices)

        # Fill gaps in occupancy df -> should be ~14400 elements afterwards                                                                                                                      
        df = df.set_index('Date_Time')                                                                                                                                                         
        df = df.resample(self.data_freq).ffill()                                                                                                                                                         
        df = df.rename_axis('Date_Time').reset_index()
        start_day = datetime(self.year, month_numeric_switch(self.month, option = "to_numeric"), self.day)
        end_day = start_day + timedelta(days=1)
        df = df[(df["Date_Time"] >= start_day) & (df["Date_Time"] < end_day)].reset_index(drop=True)
        return df

    def __save(self,data,column):
        name = column.replace(".","_").replace("/","_")
        data[["Date_Time",column]].to_hdf(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}/{self.day}/{name}.h5", key=f"{name}", mode="w")
            
    def __run(self):
        L1,dfs = self.__read_csv()
        
        if (type(L1) == type(None)) or (type(dfs) == type(None)):
            return None 

        if not os.path.isdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data"):
            os.mkdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data")

        if not os.path.isdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}"):
            os.mkdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}")

        if not os.path.isdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}"):
            os.mkdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}")

        if not os.path.isdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}/{self.day}"):
            os.mkdir(f"{os.environ.get('BASE_DIR')}/Cleaned_Data/{self.year}/{self.month}/{self.day}")

        #First resample the data
        L1 = self.__resample(L1,"5S")
        for i in range(len(dfs)):
            dfs[i] = self.__resample(dfs[i],self.data_freq)

        #Join the dfs and drop where L1 <= 100
        if len(dfs) > 1:
            joined_df = self.__join(dfs)
        else:
            joined_df = dfs[0]
        joined_df = self.__drop_L1(joined_df,L1)

        #Save the cleaned data
        columns = joined_df.columns
        columns = columns[columns != "Date_Time"]    #Remove DateTime column
        for column in columns:
            self.__save(joined_df,column)
        self.__save(L1,L1.columns[1])

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
        args.start_day = 1
        if (args.start_year == args.end_year and args.start_month > args.end_month):
            break
    args.start_year += 1
    args.start_month = 1
    
            
