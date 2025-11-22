import os
import logging
import pandas as pd
from airflow.decorators import task


# Logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_supplier_dataframe(main_df):
    '''
    Func creates dataframe with data about supplier from main dataframe
    '''
    supplier_df = main_df[
        [
            'SUPPLIER_NAME',
            'LOCATION',
            'CITY',
            'STREET',
            'BUILDING',
            'LOCALIZATION'
        ]
    ].copy()
    return supplier_df

def create_part_dataframe(main_df):
    '''
    Func creates dataframe with data about car parts from main dataframe
    '''
    part_df = main_df[
        [
            'PART_NUMBER',
            'PART_NAME',
            'PART_WEIGHT_KG'
        ]
    ].copy()
    return part_df

def create_box_dataframe(main_df):
    '''
    Func creates dataframe with data about boxes from main dataframe
    '''
    box_df = main_df[
        [
            'BOX_NUMBER',
            'BOX_TYPE',
            'BOX_WEIGHT_KG',
            'BOX_LENGTH_MM',
            'BOX_WIDTH_MM',
            'BOX_HEIGHT_MM',
            'BOX_VOL_M3',
            'BOX_AREA_M2',
            'BOX_STACKING'
        ]
    ].copy()
    return box_df

def create_pallet_dataframe(main_df):
    '''
    Func creates dataframe with data about pallets from main dataframe
    '''
    pallet_df = main_df[
        [
            'PALLET_NUMBER',
            'PALLET_TYPE',
            'PALLET_WEIGHT_KG',
            'PALLET_LENGTH_MM',
            'PALLET_WIDTH_MM',
            'PALLET_HEIGHT_MM',
            'PALLET_VOL_M3',
            'PALLET_AREA_M2',
            'PALLET_STACKING'
        ]
    ].copy()
    return pallet_df

def create_model_dataframe(main_df):
    '''
    Func creates dataframe with data about car models from main dataframe
    '''
    model_df = main_df[
        [
            'MODEL_CODE',
            'MODEL_NAME'
        ]
    ].copy()
    return model_df

def create_workshop_dataframe(main_df):
    '''
    Func creates dataframe with data about production workshops from main dataframe
    '''
    workshop_df = main_df[
        [
            'WORKSHOP_CODE',
            'WORKSHOP_NAME'
        ]
    ].copy()
    return workshop_df

@task(task_id="extractor_task")
def extractor(**context):
    '''
    The main function that moves on to the next step
    for data cleaning and transformation df_dict
    with dataframes with data of suppliers, parts,
    boxes, part quantity per one box, pallets,
    quantity boxes per one pallets, models,
    production workshops and lines.
    '''
    # Get the file path from the task configuration
    file_path = context["dag_run"].conf.get("file_path")

    # Check the file availability
    if not file_path or not os.path.exists(file_path):
        raise ValueError("Файл не найден или отсутствует путь к файлу.")

    # Read the excel file and create the main dataframe
    main_df = pd.read_excel(file_path)

    df_dict = {
        'main_df': main_df,
        'supplier_df': create_supplier_dataframe(main_df),
        'part_df': create_part_dataframe(main_df),
        'box_df': create_box_dataframe(main_df),
        'pallet_df': create_pallet_dataframe(main_df),
        'model_df': create_model_dataframe(main_df),
        'workshop_df': create_workshop_dataframe(main_df)
    }

    # Push in XCom
    context['ti'].xcom_push(key='df_dict', value=df_dict)

