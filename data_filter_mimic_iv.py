import os
import pandas as pd
import numpy as np
from develop_util.xls_process.xls_filter import xls_filter


def main() -> None:
    # 0. prepare
    base_dir = 'D:\\Dataset\\original_data\\ECG'
    dataset_name= '20240801_mimics_iv'
    sub_dataset = 'mimic-iv-ecg-ext-icd-diagnostic-labels-for-mimic-iv-ecg-1.0.0'
    file_name = 'records_w_diag_icd10.csv'
    result_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
    result_file_name = 'select_ed.csv'

    # 1. 读取 mimic iv 信息
    input_file_path = os.path.join(base_dir,dataset_name,sub_dataset,file_name)
    result_file  = os.path.join(result_dir,result_file_name)
    df = pd.read_csv(input_file_path,
                     encoding='GB18030')
    # 2. 筛选急诊 ecg 数据
    # print((1-np.isnan(df['ed_stay_id'].values)).astype(bool))
    df = df[(1-np.isnan(df['ed_stay_id'].values)).astype(bool)]
    eval_info = pd.DataFrame(df)
    eval_info.to_csv(result_file, index=False)
    # 3. 读取 mimic iv note 临床诊断报告
    note_file_name = 'D:\\Dataset\\original_data\\ECG\\20240801_mimics_iv\\mimic-iv-note\\note\\radiology.csv'
    df2 = pd.read_csv(note_file_name,
                      encoding='GB18030')
    # 4. 筛选含临床诊断报告的急诊 ecg 数据
    
    # new_df = 
    # eval_info = pd.DataFrame(new_df)

def temp():
    path_dir = 'D:\\Dataset\\original_data\\ECG\\20240801_mimics_iv\\mimic-iv-2.2\\icu'
    for filename in os.listdir(path_dir):
        print(filename)


if __name__ == '__main__':
    main()
    # temp()
