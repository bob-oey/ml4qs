import fnmatch
import json
import os
import pickle
import sys
import gzip
import time

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

# place somewhere above or in dataset folder
# rootdir = "../dataset"
rootdir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, "dataset"))
if "app_usage" not in os.listdir(rootdir):
    rootdir = os.getcwd()
    if "app_usage" not in os.listdir(rootdir):
        sys.exit()

def convert_time_intervals(df):
    if 'timestamp' not in df.columns:
        if 'start_timestamp' in df.columns and 'end_timestamp' in df.columns:
            # conversation
            df['timestamp'] = df['start_timestamp']
            df['duration_conversation'] = (df['end_timestamp'] - df['start_timestamp'])/60
            df = df.drop(columns=['start_timestamp', 'end_timestamp'])
        elif 'time' in df.columns:
            # bt, wifi or wifi_location
            df['timestamp'] = df['time']
            print(df)
            df = df.drop(columns=['time'])
            print(df)
        elif 'start' in df.columns and 'end' in df.columns:
            # dark, phonecharge, or phonelock
            df['timestamp'] = df['start']
            df['duration'] = (df['end'] - df['start'])/60
            df = df.drop(columns=['start', 'end'])


    return df


def getFolderDict(plot=False): 
    users = ["u0" + str(userid) if userid < 10 else "u"+ str(userid) for userid in list(range(0,60))]
    datatypes = {}

    for subdir, dirs, files in os.walk(rootdir):
        name = os.path.basename(subdir)
        datatypes[name] = []
        # print(name)
        for file in files:
            for user in users:
                if fnmatch.fnmatch(file, "*"+user+"*"):
                    # print(file)
                    datatypes[name].append(user)
                if fnmatch.fnmatch(file, "*"+"json") and "EMA" not in subdir:
                    # print(name)
                    pass
    
    if plot == True:
        labels = []
        follens = []

        for folder in datatypes:
            if len(datatypes[folder]) == 0:
                del folder
            else:
                labels.append(folder)
                follens.append(len(datatypes[folder]))

        y_pos = np.arange(len(labels))
        plt.bar(y_pos, follens)

        plt.xticks(y_pos, labels)
        plt.tight_layout()

        plt.show()

    return datatypes

# folderdict = getFolderDict()

def loadCSVs(foldersplit=False):
    users = ["u0" + str(userid) if userid < 10 else "u"+ str(userid) for userid in list(range(0,60))]
    # users = ["u59"]
    # datatypes = {}
    # dataframes = {}
    print("CSV parsing")
    # new df with column userid
    # df = pd.DataFrame(columns=['userid'])
    for user in users:
        df = pd.DataFrame(columns=['timestamp'])
        lastname = ""
        for subdir, dirs, files in os.walk(rootdir):
            name = os.path.basename(subdir)
            for file in files:
                if fnmatch.fnmatch(file, "*"+user+"*"+"csv") and "dinning" not in subdir and "EMA" not in subdir:
                    
                    # datatypes[name] = []
                    dirdf = pd.DataFrame()
                    # for file in files:
                        # print(file)
                        # datatypes[name].append(user)

                    dirdf = pd.read_csv(os.path.join(subdir, file), index_col=False)
                    # userdf['userid'] = user
                    # dirdf = dirdf.append(userdf)
                        # print(userdf)
                    # if "audio" in subdir:
                    #     filename = "separate_"+name+".pickle"
                    #     pickle.dump(dirdf, open(filename, "wb"))
                    #     print("Saved", filename)
                    if dirdf.empty == False:
                        print("Timeconverting", name, file)
                        dirdf = convert_time_intervals(dirdf)
                        # print("Parsing", name, "CSV:", file)
                        # dirdf['source'] = name
                        if 'timestamp' in dirdf.columns:
                            print("MERGING", name, file)
                            df = df.merge(right=dirdf, how='outer',on='timestamp', sort=True, suffixes = ( (name.replace(" ", "_"), ('_'+name.replace(" ", "_"))) ) )
                    
                    if foldersplit == True and dirdf.empty == False:
                        filename = "csv"+name+".pickle"
                        pickle.dump(df, open(file_name, "wb"))
                        print("Saved", filename)
                    # else:
                    #     dataframes[name] = dirdf
                if fnmatch.fnmatch(file, "*"+user+"*"+"json") and "calendar" not in subdir and "dinning" not in subdir:
                    with open(os.path.join(subdir, file)) as json_file:
                        json_dict = json.load(json_file)
                        
                        userdf = pd.DataFrame.from_dict(json_dict)
                        # print(name, user)
                        if userdf.empty == False:
                            userdf['resp_time'] = pd.to_datetime(userdf['resp_time'], unit='s')
                            userdf['userid'] = user
                            name = os.path.basename(subdir)
                            # print("Parsing", name, "JSON:", file)
                            userdf['source'] = name
                            if 'timestamp' in userdf.columns:
                                print("MERGING", name, file)
                                df = df.merge(right=userdf, how='outer',on='timestamp', sort=True)
            
        df['userid'] = user
        df.reset_index()
        df.set_index('timestamp')
        file_name = user+".csv"+".gz"
        df.to_csv(file_name, index=False, compression='gzip')
        
        # file_name = user+".csv"
        # df.to_csv(file_name, index=False)
        # pickle.dump(df, open(file_name, "wb"))
        print("Saved", file_name)
        # return df
    # delete = [key for key in dataframes if dataframes[key].empty]
    # for key in delete: del dataframes[key]
    # for df in dataframes:
    #     if dataframes[df].empty:
    #         print(df,"is empty")

    return 0

def loadJSONs(foldersplit=False):
    users = ["u0" + str(userid) if userid < 10 else "u"+ str(userid) for userid in list(range(0,60))]

    datatypes = {}
    dataframes = {}
    print("JSON parsing")
    for subdir, dirs, files in os.walk(rootdir):
        name = os.path.basename(subdir)
        # print(name)
        print("Parsing", name, "JSONs")
        datatypes[name] = []
        dirdf = pd.DataFrame()        
        for file in files:
            for user in users:
                if fnmatch.fnmatch(file, "*"+user+"*"+"json") and "calendar" not in subdir and "dinning" not in subdir:
                    # print(file)
                    datatypes[name].append(user)

                    with open(os.path.join(subdir, file)) as json_file:
                        json_dict = json.load(json_file)
                        
                        userdf = pd.DataFrame.from_dict(json_dict)
                        # print(name, user)
                        if not userdf.empty:
                            userdf['resp_time'] = pd.to_datetime(userdf['resp_time'], unit='s')
                            userdf['userid'] = user
                            dirdf = dirdf.append(userdf)
                        else:
                            print("Empty DF for:", name, user)
        # save dataframe pickle to folder
        if foldersplit == True and dirdf.empty == False:
            filename = "json"+name+".pickle"
            pickle.dump(dirdf, open(filename, "wb"))
            print("Saved", filename)
        else:
            dataframes[name] = dirdf
        
    delete = [key for key in dataframes if dataframes[key].empty]
    for key in delete: del dataframes[key]

    return dataframes

def makeDFs(option="both",foldersplit=False):

    print("Making", option, "dataframes")
    csvdataframe = {}
    jsondataframe = {}

    if option is not "json":
        csvdataframe = loadCSVs(foldersplit)

        # print(csvdataframe['activity'])
        if foldersplit == False:
            pickle.dump(csvdataframe, open("csvdict.pickle", "wb"))
            if option is "csv":
                return csvdataframe

    if option is not "csv":
        jsondataframe = loadJSONs(foldersplit)
    
        if foldersplit == False:
            pickle.dump(jsondataframe, open("jsondict.pickle", "wb"))
            if option is "json":
                return jsondataframe

    return csvdataframe, jsondataframe
test=loadCSVs()
# jsondf = makeDFs(option="json")
# csvdf = makeDFs(option="csv")
# csvdf = makeDFs(option='csv')


