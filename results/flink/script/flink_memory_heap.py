# -*- coding: utf-8 -*-
"""
Create diagrams out of flink jvm and memory metrics
@author Samuel Alfano
@version 1.0
"""
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import pandas as pd
import re
import math

'''
Create a new dataframe for specific data
'''
def create_dataframe(df,string_to_match):
    for index, row in df.iterrows():
        if not re.search(string_to_match, str(row[0]) ) or math.isnan(row[2]) or row[2] is 0:
            df.drop(index,inplace=True)
    
    return df 
    
path= "C:/dev/py/data"
flink_path = path + "/flink/"
flink_mean_path = flink_path + "mean/"
flink_img_path = flink_path + "img/"
flink_files = [
                "flink-exp001_2TM_Memory_used_cached.csv",
                "flink-exp001_4TM_Memory_used_cached.csv",
                "flink-exp001_8TM_Memory_used_cached.csv",
                "flink-exp002_2TM_Memory_used_cached.csv",
                "flink-exp002_4TM_Memory_used_cached.csv",
                "flink-exp002_8TM_Memory_used_cached.csv",
                "flink-exp003_2TM_Memory_used_cached.csv",
                "flink-exp003_4TM_Memory_used_cached.csv",
                "flink-exp003_8TM_Memory_used_cached.csv",
                "flink-exp004_2TM_Memory_used_cached.csv",
                "flink-exp004_4TM_Memory_used_cached.csv",
                "flink-exp004_8TM_Memory_used_cached.csv",
                "flink-exp005_2TM_Memory_used_cached.csv",
                "flink-exp005_4TM_Memory_used_cached.csv",
                "flink-exp005_8TM_Memory_used_cached.csv"
                ]


heap_path = path + "/flink/heap/"
heap_mean_path = heap_path + "mean/"
heap_files = [
                "flink-exp001_2TM_JVM_Heap_Non-Heap.csv",
                "flink-exp001_4TM_JVM_Heap_Non-Heap.csv",
                "flink-exp001_8TM_JVM_Heap_Non-Heap.csv",
                "flink-exp002_2TM_JVM_Heap_Non-Heap.csv",
                "flink-exp002_2TM_JVM_Heap_Non-Heap.csv",
                "flink-exp002_4TM_JVM_Heap_Non-Heap.csv",
                "flink-exp002_8TM_JVM_Heap_Non-Heap.csv",
                "flink-exp003_2TM_JVM_Heap_Non-Heap.csv",
                "flink-exp003_4TM_JVM_Heap_Non-Heap.csv",
                "flink-exp003_8TM_JVM_Heap_Non-Heap.csv",
                "flink-exp004_2TM_JVM_Heap_Non-Heap.csv",
                "flink-exp004_4TM_JVM_Heap_Non-Heap.csv",
                "flink-exp004_8TM_JVM_Heap_Non-Heap.csv",
                "flink-exp005_2TM_JVM_Heap_Non-Heap.csv",
                "flink-exp005_4TM_JVM_Heap_Non-Heap.csv",
                "flink-exp005_8TM_JVM_Heap_Non-Heap.csv"
                ]

# is in inches, 1cm = 2.54 inches
# Xcm / 1 inches
plt_length = 15 / 2.54
plt_height = 10 / 2.54
plt.rcParams["figure.figsize"] = (plt_length,plt_height)

for index in range(len(flink_files)):

    flink_file = flink_files[index]
    heap_file = heap_files[index]
    
    df_flink = pd.read_csv(flink_path + flink_file, sep=",", header=None)
    df_heap = pd.read_csv(heap_path + heap_file, sep=",", header=None)
    
    df_flink_used = df_flink.copy()
    df_flink_cached = df_flink.copy()
    df_heap_used = df_heap.copy()
    df_non_heap_used = df_heap.copy()
    
    
    create_dataframe(df_flink_used,'used')
    create_dataframe(df_flink_cached,'cached')
    create_dataframe(df_heap_used,'.Heap.Used')
    create_dataframe(df_non_heap_used,'.NonHeap.Used')
    
    # testing to mean values
    df_flink_used_mean = df_flink_used.groupby(1).mean().reset_index()
    df_flink_cached_mean = df_flink_cached.groupby(1).mean().reset_index()
    df_heap_used_mean = df_heap_used.groupby(1).mean().reset_index()
    df_non_heap_used_mean = df_non_heap_used.groupby(1).mean().reset_index()
    
    # Create new file
    df_flink_used_mean.to_csv(flink_mean_path + "memory.mean." + flink_file)
    df_flink_cached_mean.to_csv(flink_mean_path + "memory.cached.mean." + flink_file)
    df_heap_used_mean.to_csv(heap_mean_path + "heap.mean." + heap_file)
    df_non_heap_used_mean.to_csv(heap_mean_path + "non-heap.mean." + heap_file)
    
    # Create x data for fill_between
    x_flink_used = pd.DataFrame(np.array(range(0, len(df_flink_used_mean[2]), 1)))
    x_flink_cached = pd.DataFrame(np.array(range(0, len(df_flink_cached_mean[2]), 1)))
    x_heap_used = pd.DataFrame(np.array(range(0, len(df_heap_used_mean[2]), 1)))
    x_non_heap_used = pd.DataFrame(np.array(range(0, len(df_non_heap_used_mean[2]), 1)))
    
    fig = plt.subplot(211)
    
    plt.ylabel("Arbeitsspeicher")
    # Set y axis, 10500000 = 10.5GB
    fig.yaxis.set_major_formatter(PercentFormatter(xmax=10500000))
    fig.set_yticks(range(0,10500001, 2100000))
    plt.ylim(0,10500000)
    
    # Set for exp005, 512000 = 512MB
    #fig.yaxis.set_major_formatter(PercentFormatter(xmax=512000))
    #fig.set_yticks(range(0,512001, 102400))
    #plt.ylim(0,512000)    
    
    # Set x axis
    fig.set_xticks(range(0, 31, 3))
    fig.set_xticklabels([abs(x) for x in range(0, 180*11, 180)])
    plt.xlabel("Zeit in s")
    plt.xlim(0,30)
        
     # Print memory line
    plt.plot(df_flink_used_mean.index.values,df_flink_used_mean[2],'black', 
             df_flink_cached_mean.index.values,df_flink_cached_mean[2],'green', linewidth=1)
             
    # Print heap and non-heap line
    plt.plot(df_non_heap_used_mean.index.values,df_non_heap_used_mean[2],'purple',
             df_heap_used_mean.index.values,df_heap_used_mean[2],'red', linewidth=1)
    
    plt.fill_between(x_flink_used[0],df_flink_used_mean[2], color='#bababa')
    plt.fill_between(x_heap_used[0],df_heap_used_mean[2], color='#f9a58e')
    plt.fill_between(x_flink_cached[0],df_flink_cached_mean[2], color='#c2f4c1')
    plt.fill_between(x_non_heap_used[0],df_non_heap_used_mean[2], color='#fccff6')
    
    # Show grid, save graph to file and show
    plt.grid()
    plt.savefig(flink_img_path + flink_file + ".png")
    plt.show()