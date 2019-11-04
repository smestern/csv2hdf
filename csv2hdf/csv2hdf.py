import pandas as pd
import numpy as np
import h5py
import os
import re
import tkinter as tk
from tkinter import filedialog




def csv_to_hdfs(file_path, save):
   data = pd.read_csv(file_path)
   print(data)
   file = h5py.File(os.path.join(save,'output.h5'), 'w')
   col_to_save = data.columns.to_numpy()
   for k in col_to_save:
       coldata = data[k]
       if coldata.dtype == np.dtype('O'):
           continue
       dset = file.create_dataset(k, coldata.shape, dtype=coldata.dtype,
            compression="gzip")
       dset[...] = coldata
   file.close()

def csv_to_hdfs_group_col(file_path, gcol, save):
   data = pd.read_csv(file_path)
   print(data)
   file = h5py.File(os.path.join(save,'output.h5'), 'w')
   col_group_name = data.columns.values[gcol]
   duplicate_row = data.duplicated(data.columns.values[gcol]).to_numpy()
   data_sorted = [[] for x in range(len(data.columns.values))]

   col_to_sort = data.iloc[~duplicate_row, gcol].to_numpy()
   for l in col_to_sort:
       cell_subset = data[data[col_group_name]==l].T.to_numpy()
       for x, i in enumerate(cell_subset):
               data_sorted[x].append(i)
   
   col_to_save = data.columns.to_numpy()
   id_arr = np.hstack(col_to_sort).astype('S10')
   dset = file.create_dataset('ids', id_arr.shape, dtype="S10",
            compression="gzip")
   dset[...] = id_arr
   for i, k in enumerate(col_to_save):
       save_data = check_mismatch_size(data_sorted[i])
       coldata = np.vstack(save_data)
       if coldata.dtype == np.dtype('O'):
           continue
       k1 = k.replace('/', '')
       dset = file.create_dataset(k1, coldata.shape, dtype=coldata.dtype,
            compression="gzip")
       dset[...] = coldata
    
   file.close()  
   
def check_mismatch_size(data):
        max_len = len(max(data,key=len))
        for a, el in enumerate(data):
           len_fill = max_len - len(el)
           try:
            data[a] = np.append(el, np.full(len_fill, np.nan)).astype(np.float64)
           except:
            data[a] = np.append(el, np.full(len_fill, np.nan))
        nudata = np.vstack(data[:])
        return nudata

def equal_ar_size(array1, array2, fill):
    try:
        r1, s1 = array1.shape
        r2, s2 = array2.shape
        if s1 > s2:
            array2 = np.hstack((array2, np.full((r2, s1-s2),fill)))
        elif s2 > s1:
            array1 = np.hstack((array1, np.full((r1, s2-s1),fill)))
    except:
        s1, = array1.shape
        s2, = array2.shape
        r1 = 1
        r2 = 1
        if s1 > s2:
            array2 = np.hstack((array2, np.full((s1-s2),fill)))
        elif s2 > s1:
            array1 = np.hstack((array1, np.full((s2-s1),fill)))

    return array1, array2

