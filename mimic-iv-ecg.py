"""Module providing a function getting mimic-iv ecg info."""
import os
from logger_basic import logger
from ast import literal_eval
import numpy as np
import pandas as pd
# from develop_util.xls_process.xls_filter import xls_filter
# from config.icd_config import config
from utils import find_uppercase_words


def get_cvd_from_ecg_diag_label() -> None:
    """ 根据 mimic-iv-ecg-diagnostic-labels 的诊断结果的 ICD code,
    从 mimic-iv-ecg 数据集中筛选诊断为心血管疾病 CVD 的 case """
    # 0. prepare
    base_dir = 'D:\\Dataset\\original_data\\ECG'
    dataset_name= '20240801_mimics_iv'
    sub_dataset = 'mimic-iv-ecg-ext-icd-diagnostic-labels-for-mimic-iv-ecg-1.0.0'
    file_name = 'records_w_diag_icd10.csv'
    result_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    result_file_name = '01_ecg_cvd.csv'
    # 1. 读取 mimic iv ecg label 临床诊断标签 - ICD code
    file_name = os.path.join(base_dir,dataset_name,sub_dataset,file_name)
    df = pd.read_csv(file_name)
    new_df = {}
    for key in df.keys():
        new_df[key] = []
    all_diag = df['all_diag_all'].values

    ## 2. 根据 icd code 筛选 CVD 患者
    for idx, diag in enumerate(all_diag):
        diag = literal_eval(diag)
        initials = [sub_diag[0] for sub_diag in diag]
        if 'I' in initials:
            for colume in df.keys():
                new_df[colume].append(df[colume][idx])

    result_file  = os.path.join(result_dir,result_file_name)
    eval_info = pd.DataFrame(new_df)
    eval_info.to_csv(result_file, index=False)


def filter_diag_beside_cvd() -> None:
    """ mimic-iv-ecg-cvd 中只保留 cvd 的 icd code 表格 """
    # 0. prepare
    file_name = '01_ecg_cvd.csv'
    result_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    result_file_name = '02_ecg_filter_cvd.csv'
    file_name = os.path.join(result_dir,file_name)
    df = pd.read_csv(file_name)
    columes = ['ed_diag_ed','ed_diag_hosp','hosp_diag_hosp','all_diag_hosp','all_diag_all']
    ## 1. 筛选 CVD 疾病的 code
    for colume in columes:
        for idx, value in enumerate(df[colume]):
            new_diag = [diag for diag in literal_eval(value) if diag[0]=='I']
            df.loc[idx, colume] = str(new_diag)
    result_file  = os.path.join(result_dir,result_file_name)
    eval_info = pd.DataFrame(df)
    eval_info.to_csv(result_file, index=False)


def identify_cvd_sub_class() -> None:
    """ CVD 中补充识别 缺血性心肌病(ICM)  """
    # 0. prepare
    file_name = '02_ecg_filter_cvd.csv'
    result_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    result_file_name = '03_ecg_cvd_is_icm.csv'
    file_name = os.path.join(result_dir,file_name)
    df = pd.read_csv(file_name)
    is_i20_25 = ['I20','I21','I22','I23','I24','I25']
    df['icd_class'] = np.empty(len(df), dtype=object)
    df['is_i20_25'] = np.empty(len(df), dtype=object)
    ## 1. 筛选 CVD 疾病的 code
    for idx, value in enumerate(df['all_diag_all']):
        new_diag = [diag[0:3] for diag in literal_eval(value)]
        df.loc[idx, 'icd_class'] = str(list(set(new_diag)))
        if np.isin(is_i20_25, new_diag).any():
            df.loc[idx, 'is_i20_25'] = 1
        else:
            df.loc[idx, 'is_i20_25'] = 0
    result_file  = os.path.join(result_dir,result_file_name)
    eval_info = pd.DataFrame(df)
    eval_info.to_csv(result_file, index=False)
    logger.info('%s patient in i20_25', len(set(df['subject_id']))) # 82300
    logger.info('%s ecg event in i20_25', np.sum(df['is_i20_25'])) # 157974


def get_ed_icm_and_hosp_icm() -> None:
    """ 将缺血性心肌病(ICM)的 patient&event 筛选出来, 并按住院和急诊, 分为两个别表 """
    # 0. prepare
    file_name = '03_ecg_cvd_sub_class.csv'
    result_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    result_file_name1 = '04_ecg_ed_icm.csv'
    result_file_name2 = '04_ecg_hosp_icm.csv'
    file_name = os.path.join(result_dir,file_name)
    df = pd.read_csv(file_name)
    # 1. 急诊住院的缺血性心肌病
    df1 = df[(1-np.isnan(df['ed_stay_id'].values)).astype(bool)]
    df2 = df1[df1['is_i20_25']==1]
    result_file  = os.path.join(result_dir,result_file_name1)
    del df2['is_i20_25']
    eval_info = pd.DataFrame(df2)
    eval_info.to_csv(result_file, index=False)
    logger.info('%s patient in i20_25 & from ed', len(set(df2['subject_id']))) # 15707
    logger.info('%s ecg event in i20_25 & from ed', np.sum(df2['is_i20_25'])) # 39012
    # 2. 医院住院的缺血性心肌病
    df1 = df[(np.isnan(df['ed_stay_id'].values)).astype(bool)]
    df2 = df1[df1['is_i20_25']==1]
    result_file  = os.path.join(result_dir,result_file_name2)
    del df2['is_i20_25']
    eval_info = pd.DataFrame(df2)
    eval_info.to_csv(result_file, index=False)
    logger.info('%s patient in i20_25 & from hosp', len(set(df2['subject_id']))) # 25796
    logger.info('%s ecg event in i20_25 & from hosp', np.sum(df2['is_i20_25'])) # 118962


def main():
    """ 处理 mimic_iv_ecg 数据 """
    # 1. 根据 mimic-iv-ecg-diagnostic-labels 的诊断结果的 ICD code,
    # 从 mimic-iv-ecg 数据集中筛选诊断为心血管疾病 CVD 的 case
    get_cvd_from_ecg_diag_label()
    # 2. mimic-iv-ecg-cvd 中只保留 cvd 的 icd code 表格
    filter_diag_beside_cvd()
    # 3. CVD 中补充识别 缺血性心肌病(ICM)
    identify_cvd_sub_class()
    # 4. 将缺血性心肌病 ICM 的 patient&event 筛选出来, 并按住院和急诊, 分为两个别表
    get_ed_icm_and_hosp_icm()

if __name__ == '__main__':
    main()
