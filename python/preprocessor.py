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
parser.add_argument('-ms', '--max_smooth', action = 'append', help='Max_smooth variables')
args = parser.parse_args()

class preprocessor_pbeast:

    def __init__(self, day, month, year, patterns=[],max_smooth=[],interval_freq="6S",smooth_freq = "6S",how_interpolate="linear"):
        self.patterns = patterns
        self.day = day
        self.month = month
        self.year = year
        self.max_smooth = max_smooth
        self.interval_freq = interval_freq
        self.smooth_freq = "6S"
        self.how_interpolate = how_interpolate
        self.data = self.__transform()
    
    def __read_csv(self):
        dfs = []
        for pattern in self.patterns:
            for files in glob.glob(f"{os.environ.get('BASE_DIR')}/Data/{self.year}/{self.month}/{self.day}/{pattern}"):
                dfs.append(pd.read_csv(files, delimiter=","))
        return dfs
    
    def __month_to_numeric(self,dfs):
        month_dict = {"Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04", "May":"05", "Jun":"06", 
             "Jul":"07", "Aug":"08", "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12"}     
        for df in dfs:
            df["Date_Time"].replace(month_dict,regex=True, inplace = True)

    
    def __interval_to_points(self,dfs):
        for i,df in enumerate(dfs):
            from_to_rows = df.loc[df['Date_Time'].str.contains(" - ")].index     #Find rows where datetime is given as interval
            for index in reversed(from_to_rows):     #Reverse so index does not change
                tmp_start = df.iloc[:index].copy()
                tmp_end = df.iloc[index+1:].copy()

                start = df.iloc[index]["Date_Time"].split(" - ")[0]
                end = df.iloc[index]["Date_Time"].split(" - ")[1]

                datetimeindex = pd.date_range(start,end,freq=self.interval_freq,inclusive = "left")
                to_be_appended = pd.DataFrame({df.columns[0]:datetimeindex, 
                                               df.columns[1]:np.ones(len(datetimeindex))*df.iloc[index][df.columns[1]]})

                df = pd.concat([tmp_start,to_be_appended,tmp_end], ignore_index = True)

            df[df.columns[0]]= pd.to_datetime(df[df.columns[0]]) #Cast to datetime object
            dfs[i] = df
    
    def __smoothing(self,dfs,freq):
        resampled = []
        for i,df in enumerate(dfs):
            col = np.array(df.columns)
            new_cols = np.delete(col, np.where(col == "Date_Time"))
            #if self.max_smooth != None:
            #    for max_pattern in self.max_smooth:
            #        max_smooth_cols = []
            #        dfs[i] = df.copy().resample(freq,on="Date_Time")[col].max()
            start_day = datetime(self.year, 5, self.day)
            end_day = start_day + timedelta(days=1)
            dfs[i] = df[(df["Date_Time"] >= start_day) & (df["Date_Time"] < end_day)]
            dfs[i] = dfs[i].resample(freq, on="Date_Time")[new_cols].mean()
            dfs[i].reset_index(inplace=True)
        #return dfs
        
    def __joiner(self,dfs):
        joined_df=dfs[0]
        for i in range(1,len(dfs)):
            print(joined_df.dtypes)
            print(dfs[i].dtypes)
            joined_df = joined_df.join(dfs[i].copy(), how="outer", on="Date_Time")
        joined_df.sort_values(by="Date_Time", inplace=True)
        joined_df.reset_index(inplace=True)
        return joined_df
    
    def __interpolator(self,df,how):
        col = np.array(df.columns)
        new_cols = np.delete(col, np.where(col == "Date_Time"))
        df[new_cols] = df[new_cols].interpolate(method=how, limit_direction="both", axis=0)
        return df
    
    def __transform(self):
        initial_dfs = self.__read_csv()
        
        if initial_dfs:
            self.__month_to_numeric(initial_dfs)
            self.__interval_to_points(initial_dfs)
            self.__smoothing(initial_dfs,self.smooth_freq)
            
            joined_dfs = self.__joiner(initial_dfs)
            joined_dfs = self.__interpolator(joined_dfs,self.how_interpolate)
        else:
            joined_dfs = pd.DataFrame()
        return joined_dfs


month_dict = {1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun", 7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dez"}
while(args.start_year <= args.end_year):
    while(args.start_month <= 12):
        while(args.start_day <= calendar.monthrange(args.start_year, args.start_month)[1]):
            if (args.start_year == args.end_year and args.start_month == args.start_month and args.start_day > args.end_day):
                break
            preprocessor_pbeast(day=args.start_day, month=month_dict[args.start_month], year=args.start_year, patterns=args.patterns, max_smooth=args.max_smooth)
            args.start_day += 1
        args.start_month += 1
        if (args.start_year == args.end_year and args.start_month > args.end_month):
            break
    args.start_year += 1
    args.start_month = 1
