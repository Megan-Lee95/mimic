"""Module providing a function getting mimic-iv note info."""
import os
from logger_basic import logger
from ast import literal_eval
import numpy as np
import pandas as pd
# from develop_util.xls_process.xls_filter import xls_filter
# from config.icd_config import config
from utils import find_uppercase_words


# mimic-iv-note
def arrange_diag_code_of_note() -> None:
    """
    TODO: mimic-iv-note radiology 中 exam code 与官网所述的 CPT code 不符
    手动整理检查项目
    """
    # 0. prepare
    base_dir = 'D:\\Dataset\\original_data\\ECG'
    dataset_name= '20240801_mimics_iv'
    sub_dataset = 'mimic-iv-note'
    note_name = 'note\\radiology_detail.csv'
    result_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    result_file_name = 'A1_cpt_exam_arrange.csv'
    # 1. 读取 mimic-iv-note 数据列表
    note_file_name = os.path.join(base_dir,dataset_name,sub_dataset,note_name)
    note_df = pd.read_csv(note_file_name)
    note_df['unique_id'] = note_df['note_id'].values + \
                           '-' + \
                           note_df['field_ordinal'].values.astype(str)
    # 2. 处理 cpt_code
    new_df = {}
    new_column = ['note_id','subject_id','exam_code','exam_name','cpt_code','field_ordinal']
    # new_df['unique_id'] = np.array(list(set(note_df['unique_id'].values)))
    for key in new_column:
        new_df[key] = []
        # new_df[key] = np.empty(len(note_df['unique_id']), dtype=object)
    cpt_df = note_df[note_df['field_name']=='cpt_code']
    del cpt_df['field_name']
    # TODO: df 函数不能解决：cpt_df.rename(columns={'field_value': 'cpt_code'})
    cpt_df['cpt_code'] = cpt_df['field_value'][:]
    del cpt_df['field_value']
    cpt_df['exam_code'] = np.empty(len(cpt_df), dtype=object)
    cpt_df['exam_name'] = np.empty(len(cpt_df), dtype=object)
    # 3. 处理 exam_code
    exam_df = note_df[(note_df['field_name']=='exam_code')|(note_df['field_name']=='exam_name')]
    sorted_df = exam_df.sort_values(by=['note_id', 'field_ordinal', 'field_name'],
                                    ascending=[True, True, True])
    sorted_df['exam_name'] = sorted_df['field_value']
    sorted_df['exam_name'][:-1] = sorted_df['exam_name'].values[1:]
    sorted_df = sorted_df[sorted_df['field_name']=='exam_code']
    del sorted_df['field_name']
    # TODO: df 函数不能解决：sorted_df.rename(columns={'field_value': 'exam_code'})
    sorted_df['exam_code'] = sorted_df['field_value'][:]
    del sorted_df['field_value']
    final_df = pd.concat([cpt_df, sorted_df])

    result_file  = os.path.join(result_dir,result_file_name)
    eval_info = pd.DataFrame(final_df)
    eval_info.to_csv(result_file, index=False)


def filter_diag_code_of_note() -> None:
    """ 获取全部检查项目 list """
    # 0. prepare
    base_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    file_name = 'A1_cpt_exam_arrange.csv'
    result_file_name = 'A2_uniqe_cpt_exam.xlsx'
    # 1. 读取 mimic-iv-note 数据列表
    file_name = os.path.join(base_dir, file_name)
    df = pd.read_csv(file_name)
    del df['note_id'], df['subject_id'], df['field_ordinal'], df['unique_id']
    df = df.drop_duplicates() # 剔除重复行
    df = df[df['exam_name']!='___']
    df = df.sort_values(by=['exam_code'], ascending=[True])
    # 2. save
    result_file  = os.path.join(base_dir,result_file_name)
    eval_info = pd.DataFrame(df)
    eval_info.to_excel(result_file, index=False)
    # 手动 google 翻译


def filter_ecg_note_from_mimic_iv_note() -> None:
    """ 从 mimic-iv-note 中筛选与 mimic-iv-ecg 对应患者的 note """
    # 0. prepare
    base_dir = 'D:\\Dataset\\original_data\\ECG'
    dataset_name= '20240801_mimics_iv'
    sub_dataset1 = 'mimic-iv-note'
    sub_dataset2 = 'mimic-iv-ecg-1.0'
    file_name1 = 'note\\radiology.csv'
    file_name2 = 'record_list.csv'
    result_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    result_file_name = '08_ecg_note.csv'
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
    logger.info('%s ecg patient got note', np.sum(len(np.unique(ecg_note_df['subject_id'].values)))) # 146281


# TODO:
def select_ecg_eddiag_note() -> None:
    """ 筛选同时含有 mimic-iv-ecg, ed-diag, note 的数据 """
    # 0. prepare
    base_dir = 'D:\\Dataset\\original_data\\ECG'
    dataset_name= '20240801_mimics_iv'
    sub_dataset1 = 'mimic-iv-note'
    sub_dataset2 = 'mimic-iv-ecg-1.0'
    file_name1 = 'note\\radiology.csv'
    file_name2 = 'record_list.csv'
    result_dir = 'D:\\BaiduSyncdisk\\home\\DatasetAnalysisDoc\\ecg\\mimic'
    result_file_name = '08_ecg_note.csv'
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
    logger.info('%s ecg patient got note',
                np.sum(len(np.unique(ecg_note_df['subject_id'].values)))) # 146281


def main():
    """ 处理 mimic_iv_note 的诊断报告 """
    # A1. mimic-iv-note radiology_detile, 手动整理检查项目 code
    arrange_diag_code_of_note()
    # A2. 获取全部检查项目 list
    filter_diag_code_of_note()
    # 8. 从 mimic-iv-note 中筛选与 mimic-iv-ecg 对应患者的 note
    filter_ecg_note_from_mimic_iv_note()
    # TODO:9. 筛选同时含有 mimic-iv-ecg, ed-diag, note 的数据，未调试完
    select_ecg_eddiag_note()

if __name__ == '__main__':
    main()