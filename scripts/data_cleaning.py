import os
import argparse
import pandas as pd

def to_filename(line):
    new_line = line.replace('.', '_')
    new_line = new_line.replace('\n', '')
    return new_line.replace('/', '_')

def to_values(line):
    new_line = line.split('[', 1)[1]
    return new_line.replace('] ', ',')
    
#Take Commando line arguments: Day, Month, Year and Pattern 
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--day', type = int, required=True)
parser.add_argument('-m', '--month', type = str, required=True)
parser.add_argument('-y', '--year', type = int, required=True)
parser.add_argument('-p', '--pattern', type = str, required=True, help='Pattern to split files')
args = parser.parse_args()

base_dir = os.environ.get('BASE_DIR')    #Get the Base dir set by the setup script


new_f = None    #Initialise new_f -> file to write in
filename = None #initialise filename

with open(f'{base_dir}/tmp.csv') as f:    #Read file line by line
    for line in f:
        if (args.pattern in line):

            if(new_f != None):
                new_f.close()
                data = pd.read_csv(f'{base_dir}/Data/{args.year}/{args.month}/{args.day}/{filename}.csv',delimiter=",")
                data.to_hdf(f'{base_dir}/Data/{args.year}/{args.month}/{args.day}/{filename}.h5', key=f"{filename}", mode="w")
                os.remove(f'{base_dir}/Data/{args.year}/{args.month}/{args.day}/{filename}.csv')

            line = line.replace(":","")
            filename = to_filename(line)
            new_f = open(f'{base_dir}/Data/{args.year}/{args.month}/{args.day}/{filename}.csv', 'a')    #Create new file if pattern found --> New module
            new_f.write('Date_Time,'+line)

        elif (new_f != None):
            cleaned_line = to_values(line)

            new_f.write(cleaned_line)
if new_f != None:
    new_f.close()
    data = pd.read_csv(f'{base_dir}/Data/{args.year}/{args.month}/{args.day}/{filename}.csv',delimiter=",")
    data.to_hdf(f'{base_dir}/Data/{args.year}/{args.month}/{args.day}/{filename}.h5', key=f"{filename}", mode="w")
    os.remove(f'{base_dir}/Data/{args.year}/{args.month}/{args.day}/{filename}.csv')
