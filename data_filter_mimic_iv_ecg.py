import os
import sys
sys.path.append(os.getcwd())
import pandas as pd
import numpy as np
import json
import ast
from develop_util.xls_process.xls_filter import xls_filter
from mimic.config.config import *
import icd-mappings

STE_list = ['ST-T changes','ST elevation','T wave changes','','','','',]

def main() -> None:
    # 0. 读取全部 ptb 诊断表格
    base_dir = 'D:\Dataset\original_data\ECG'
    dataset = 'mimics'
    version = 'mimic-iv-ecg-matched-subset-1.0'
    file_name = 'machine_measurements.csv'
    file_path = os.path.join(base_dir,dataset,version,file_name)
    # df = pd.read_csv(file_path, encoding='GB2312')
    df = pd.read_csv(file_path)
    subject_ids = df['subject_id'].values

    # _matrix = []
    # for i in range(18):
    #     column = 'report_'+str(i)
    #     total_list = np.unique((df[column]).astype(np.str_))
    #     # print('total_list',total_list)
    #     _matrix.extend(total_list)

    # # 2. 搭建待写入的表格
    new_df = {}
    new_df['subject_id'] = df['subject_id']
    new_df['study_id'] = df['study_id']
    new_df['cart_id'] = df['cart_id']
    for key in scp_statements.keys():
        new_df[key] = []
    new_df['middle_class'] = []
    new_df['sub_class'] = []
    new_df['NSTE'] = []
    for key in I_want_config:
        new_df[key] = []

    for idx, subject_id in enumerate(subject_ids):
        # 准备写入的当前行
        sub_df = {v:[] for v in new_df.keys()}
        sub_df['SR'] = [1]
        for key in scp_statements.keys():
            sub_df[key] = [0]
        sub_df['NSTE'] = [0]

        # 开始读取当前行
        for i in range(18):
            column = 'report_'+str(i)
            df[column][idx]

        for key in scp_dict.keys():
            # 1） 写入类别
            if key in sub2super.keys():
                sub_df[sub2super[key]] = [1]
                sub_df['middle_class'].append(middl_ids[sub2middle[key]])
                sub_df['sub_class'].append(sub_ids[key])
            # 2） 写入其他值
            if key in I_want_config:
                sub_df[key] = [scp_dict[key]]
            if key == 'NST_':
                sub_df['NSTE'] = [1]
        for key in sub_df.keys():
            if key not in ['ecg_id','patient_id','report']:
                new_df[key].append(str(sub_df[key])[1:-1])
            else:
                continue
    MI_number = np.count_nonzero(new_df['MI'])
    STTC_number = np.count_nonzero(new_df['STTC'])
    
    result_file  = 'D:\DatasetAnalysisDoc\mimics\\get_total_report.xlsx'
    eval_info = pd.DataFrame(new_df)
    eval_info.to_csv(result_file, index=False)

if __name__ == '__main__':
    main()
