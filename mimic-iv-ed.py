"""Module providing a function getting mimic-iv note info."""
import os
from logger_basic import logger
from ast import literal_eval
import numpy as np
import pandas as pd
# from develop_util.xls_process.xls_filter import xls_filter
# from config.icd_config import config
from utils import find_uppercase_words


# mimic-iv-ed
def get_cvd_from_ed() -> None:
    """ 从 mimic-iv-ed 数据集中, 筛选所有 CVD 患者 """
    # 0. prepare
    base_dir = 'D:\\Dataset\\original_data\\ECG'
    dataset_name= '20240801_mimics_iv'
    sub_dataset = 'mimic-iv-ed-2.2'
    file_name = 'ed\\diagnosis.csv'
    # 1. 读取 ed event 数据列表, 按 icd_code 检索缺血性心肌病
    file_path = os.path.join(base_dir,dataset_name,sub_dataset,file_name)
    ed_diagnosis_df = pd.read_csv(file_path, encoding='GB18030')
    result_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    result_file_name = '05_ed_cvd.csv'
    cvd_df = {}
    for key in ed_diagnosis_df.keys():
        cvd_df[key] = []

    # 2. 根据 icd code 筛选 CVD 患者
    all_diags = ed_diagnosis_df['icd_code'].values
    for idx, diag in enumerate(all_diags):
        ## icd version = 9
        if ed_diagnosis_df['icd_version'][idx] == 9:
            if diag.isnumeric():
                class_id = int(str(diag)[0:2])
                if class_id>=39 and class_id<46:
                    for key in ed_diagnosis_df.keys():
                        cvd_df[key].append(ed_diagnosis_df[key][idx])
        ## icd version = 10
        elif ed_diagnosis_df['icd_version'][idx] == 10:
            if 'I' in diag[0]:
                for key in ed_diagnosis_df.keys():
                    cvd_df[key].append(ed_diagnosis_df[key][idx])
    # 3. save
    result_file  = os.path.join(result_dir,result_file_name)
    eval_info = pd.DataFrame(cvd_df)
    eval_info.to_csv(result_file, index=False)
    logger.info('%s cvd patient in mimic-iv-ed', len(np.unique(cvd_df['subject_id']))) # 48973
    logger.info('%s cvd case in mimic-iv-ed', len(cvd_df['subject_id'])) # 91244


def get_icm_from_cvd_ed() -> None:
    """ 从 mimic-iv-ed 数据集中, 筛选所有 CVD 患者 """
    # 0. prepare
    base_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    file_name = '05_ed_cvd.csv'
    file_path = os.path.join(base_dir,file_name)
    cvd_df = pd.read_csv(file_path, encoding='GB18030')
    icm_df = {}
    for key in cvd_df.keys():
        icm_df[key] = []
    # 1. 根据 icm code 分开标记 CVD 患者
    all_diags = cvd_df['icd_code'].values
    for idx, diag in enumerate(all_diags):
        ## icd version = 9
        if cvd_df['icd_version'][idx] == 9:
            class_id = int(str(diag)[0:3])
            if class_id>=410 and class_id<=414:
                for key in cvd_df.keys():
                    icm_df[key].append(cvd_df[key][idx])
        ## icd version = 10
        elif cvd_df['icd_version'][idx] == 10:
            class_id = int(diag[1:3])
            if class_id>=20 and class_id<=25:
                for key in cvd_df.keys():
                    icm_df[key].append(cvd_df[key][idx])
    # 2. save
    result_file_name = '06_ed_icm.csv'
    result_file  = os.path.join(base_dir,result_file_name)
    eval_info = pd.DataFrame(icm_df)
    eval_info.to_csv(result_file, index=False)
    logger.info('%s icm patient in mimic-iv-ed', len(np.unique(icm_df['subject_id']))) # 48973
    logger.info('%s icm case in mimic-iv-ed', len(icm_df['subject_id'])) # 91244


def filter_diag() -> None:
    """ 获取 icd-9、icd-10 诊断的结果 """
    # 0. prepare
    base_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    file_name = '06_ed_icm.csv'
    result_file_name = 'A3_ed_icm_diagnosis_classes.csv'
    # 1. 读取 mimic-iv-ed-icm 数据列表
    file_name = os.path.join(base_dir, file_name)
    df = pd.read_csv(file_name)
    diag_df = {'icd_version':[],
               'diag_classes':[],
               'counts':[]}
    for i in [9,10]:
        icd_df = df[df['icd_version']==i]
        diag_classes, counts = np.unique(icd_df['icd_title'], return_counts=True)
        diag_df['icd_version'].extend([i]*len(diag_classes))
        diag_df['diag_classes'].extend(list(diag_classes))
        diag_df['counts'].extend(list(counts))

    logger.info('mimic-iv-ed icm diagnosis: %s', diag_df['diag_classes']) # 共43个类别
    # 2. save
    result_file  = os.path.join(base_dir,result_file_name)
    eval_info = pd.DataFrame(diag_df)
    eval_info.to_csv(result_file, index=False)


def filter_ecg_diag_from_mimic_iv_ed() -> None:
    """ 从 mimic-iv-ed 中筛选与 mimic-iv-ecg 对应患者的 diagnosis """
    # 0. prepare
    base_dir = 'D:\\Dataset\\original_data\\ECG'
    dataset_name= '20240801_mimics_iv'
    sub_dataset1 = 'mimic-iv-ed-2.2'
    sub_dataset2 = 'mimic-iv-ecg-1.0'
    file_name1 = 'ed\\diagnosis.csv'
    file_name2 = 'record_list.csv'
    result_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    result_file_name = '07_ecg_ed_diag.csv'
    # 1. 读取 mimic iv note & mimic iv ecg 列表
    file_name1 = os.path.join(base_dir,dataset_name,sub_dataset1,file_name1)
    note_df = pd.read_csv(file_name1)
    file_name2 = os.path.join(base_dir,dataset_name,sub_dataset2,file_name2)
    ecg_df = pd.read_csv(file_name2)
    # 2. 读取 mimic iv ecg subjuct_id，并 filter note
    subject_ids = np.unique(ecg_df['subject_id'].values)
    ecg_note_df = note_df[np.isin(note_df['subject_id'],subject_ids)]
    # 3. save
    result_file  = os.path.join(result_dir,result_file_name)
    eval_info = pd.DataFrame(ecg_note_df)
    eval_info.to_csv(result_file, index=False)
    logger.info('%s patient in mimic-iv-ecg', len(subject_ids)) # 161352
    logger.info('%s ecg patient got ed diag', np.sum(len(np.unique(ecg_note_df['subject_id'].values)))) # 110176


def main():
    """ 处理 mimic_iv_ed 数据 """
    # 5. 从 ed event 数据集中, 筛选所有 CVD 患者
    get_cvd_from_ed()
    # 6. 从 ed event 数据的 CVD 患者中，筛选 ICM 患者
    get_icm_from_cvd_ed()
    # A3. 获取全部 ED ICM 诊断结果 list
    filter_diag()
    # 7. 从 mimic-iv-ed 中筛选与 mimic-iv-ecg 对应患者的 diagnosis
    filter_ecg_diag_from_mimic_iv_ed()


if __name__ == '__main__':
    main()