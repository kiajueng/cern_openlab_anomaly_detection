import numpy as np
import pandas as pd
import glob
import os

class preprocessor_pbeast:

    def __init__(self,dfs=[],path="",max_smooth=[],interval_freq="6S",smooth_freq = "6S",how_interpolate="linear"):
        self.dfs = dfs
        self.path = os.environ.get("BASE_DIR") if (path == "") else path
        self.max_smooth = max_smooth
        self.interval_freq = interval_freq
        self.smooth_freq = "6S"
        self.how_interpolate = how_interpolate
        self.data = self.__transform()
    
    def __read_csv(self):
        dfs = []
        for files in glob.glob(self.path + "/*.csv"):
            dfs.append(pd.read_csv(files, delimiter=","))
        return dfs
    
    def __month_to_numeric(self,dfs):
        month_dict = {"Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04", "May":"05", "June":"06", 
             "July":"07", "Aug":"08", "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12"}     
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
            col = list(df.columns)
            col.remove("Date_Time")
            if any(element in col for element in self.max_smooth):        # Check if pattern in max_smooth is in col 
                dfs[i] = df.copy().resample(freq,on="Date_Time")[col].max()
            else:
                dfs[i] = df.copy().resample(freq, on="Date_Time")[col].mean()

    def __joiner(self,dfs):
        joined_df=dfs[0].copy()
        for i in range(1,len(dfs)):
            joined_df = joined_df.join(dfs[i].copy(), how="outer", on="Date_Time")
        joined_df.reset_index(inplace=True)
        joined_df.sort_values(by="Date_Time", inplace=True)
        return joined_df
    
    def __interpolator(self,df,how):
        interpolated = df.interpolate(method=how, limit_direction="both", axis=0)
        return interpolated
    
    def __transform(self):
        initial_dfs = self.dfs if self.dfs else self.__read_csv()
        if type(initial_dfs) != list:
            raise TypeError("The initial dataframes are not in a list!")
            
        if initial_dfs:
            self.__month_to_numeric(initial_dfs)
            self.__interval_to_points(initial_dfs)
            self.__smoothing(initial_dfs,self.smooth_freq)

            joined_dfs = self.__joiner(initial_dfs)
            joined_dfs = self.__interpolator(joined_dfs,self.how_interpolate)
        else:
            joined_dfs = pd.DataFrame()
        return joined_dfs
        
