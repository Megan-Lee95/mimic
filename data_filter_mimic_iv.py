"""Module providing a function filtering mimic-iv data."""
import os
from logger_basic import logger
from ast import literal_eval
import numpy as np
import pandas as pd
from develop_util.xls_process.xls_filter import xls_filter
from config.icd_config import config
from utils import find_uppercase_words

# mimic-iv-ecg
def get_cvd_from_ecg_diag_label() -> None:
    """ 根据 mimic-iv-ecg-diagnostic-labels 的诊断结果的 ICD code,
    从 mimic-iv-ecg 数据集中筛选诊断为心血管疾病 CVD 的 case """
    # 0. prepare
    base_dir = 'D:\\Dataset\\original_data\\ECG'
    dataset_name= '20240801_mimics_iv'
    sub_dataset = 'mimic-iv-ecg-ext-icd-diagnostic-labels-for-mimic-iv-ecg-1.0.0'
    file_name = 'records_w_diag_icd10.csv'
    result_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    result_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    result_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    result_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    result_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    base_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    base_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    result_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    result_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    base_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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
    result_dir = 'D:\\Dataset\\standardized_data\\ECG\\mimic'
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

def mimic_iv_ecg_process():
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

def mimic_iv_ed_process():
    """ 处理 mimic_iv_ed 数据 """
    # 5. 从 ed event 数据集中, 筛选所有 CVD 患者
    get_cvd_from_ed()
    # 6. 从 ed event 数据的 CVD 患者中，筛选 ICM 患者
    get_icm_from_cvd_ed()
    # A3. 获取全部 ED ICM 诊断结果 list
    filter_diag()
    # 7. 从 mimic-iv-ed 中筛选与 mimic-iv-ecg 对应患者的 diagnosis
    filter_ecg_diag_from_mimic_iv_ed()

def mimic_iv_note_process():
    """ 处理 mimic_iv_note 的诊断报告 """
    # # A1. mimic-iv-note radiology_detile, 手动整理检查项目 code
    # arrange_diag_code_of_note()
    # # A2. 获取全部检查项目 list
    # filter_diag_code_of_note()
    # 8. 从 mimic-iv-note 中筛选与 mimic-iv-ecg 对应患者的 note
    filter_ecg_note_from_mimic_iv_note()

if __name__ == '__main__':
    # mimic_iv_ecg_process()
    # mimic_iv_ed_process()
    mimic_iv_note_process()
    # temp()
