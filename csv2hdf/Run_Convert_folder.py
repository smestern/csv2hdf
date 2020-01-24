import csv2hdf
import pandas as pd
import numpy as np
import h5py
import os
import re
import tkinter as tk
from tkinter import filedialog

root = tk.Tk()
root.withdraw()
file_path = filedialog.askdirectory()
save_path,_ = os.path.split(file_path)
csv2hdf.folder_csv_to_hdfs_group_col(file_path, save_path)

print("conversion complete")