"""Module providing a function filtering mimic-iv data."""
import os
from logger_basic import logger
from ast import literal_eval
import numpy as np
import pandas as pd
# from develop_util.xls_process.xls_filter import xls_filter
# from config.icd_config import config
from utils import find_uppercase_words

# other
def todo() -> None:
    """
    TODO: mimic-iv-note radiology 中 exam code 与官网所述的 CPT code 不符
    手动整理检查项目
    """
    text_classes = ['EXAMINATION','INDICATION','TECHNIQUE','COMPARISON','FINDINGS',
                    'PROCEDURE','HISTORY','IMPRESSION']
    uppercase_words = []
    for idx, text in enumerate(note_df['text'][1:100000:1000]):
        uppercase_words.extend(find_uppercase_words(text))
    uppercase_words = list(set(uppercase_words))
    print('uppercase_words',uppercase_words)
    examination = ''
    technique = ''
    for line in text.splitlines():
        if 'EXAMINATION' in line:
            examination = line.split(':')[-1].lstrip()
            s += 1
        elif 'TECHNIQUE' in line:
            technique = line.split(':')[-1].lstrip()
            s += 1
        else:
            continue
    if s==0:
        for _, colume in enumerate(new_column[:-2]):
            if colume in df.keys():
                trouble_table[colume].append(df[colume][idx])
        trouble_table['text'].append(df['text'][idx])
    else:
        for _, colume in enumerate(new_column):
            if colume in df.keys():
                new_df[colume].append(df[colume][idx])
        new_df['examination'].append(examination)
        new_df['technique'].append(technique)

def temp():
    """临时文件"""
    path_dir = 'D:\\Dataset\\original_data\\ECG\\20240801_mimics_iv\\mimic-iv-ed-2.2\\ed'
    for filename in os.listdir(path_dir):
        print(filename)

if __name__ == '__main__':
    temp()