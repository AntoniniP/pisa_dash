import logging

from lib import data_manager

logging.basicConfig(level=logging.DEBUG)

paths = dict(
    CY07_VNM_STU_COG='data/SPSS_VNM_PV_COG/CY07_VNM_STU_COG.sav',  # Additional data file for Viet Nam
    CY07_VNM_STU_PVS='data/SPSS_VNM_PV_COG/CY07_VNM_STU_PVS.sav',  # Additional data file for Viet Nam
    CY07_MSU_SCH_QQQ='data/CY07_MSU_SCH_QQQ.sav',  # School questionnaire data file
    CY07_MSU_STU_COG='data/CY07_MSU_STU_COG.sav',  # Cognitive item data file
    CY07_MSU_STU_QQQ='data/CY07_MSU_STU_QQQ.sav',  # Student questionnaire data file
    CY07_MSU_STU_TIM='data/CY07_MSU_STU_TIM.sav',  # Questionnaire timing data files
    CY07_MSU_TCH_QQQ='data/CY07_MSU_TCH_QQQ.sav'  # Teacher questionnaire data file
)

if __name__ == '__main__':
    print('Hello, world!')

    # data_manager.import_spss(paths['CY07_MSU_SCH_QQQ'], 'CY07_MSU_SCH_QQQ')

    for k, v in paths.items():
        logging.info("Importing {}".format(k))
        data_manager.import_spss(v, k)
