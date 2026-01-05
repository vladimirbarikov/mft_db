"""
Data Extraction Module for MFT Database Project

This module handles data extraction from Excel source files and creates specialized
DataFrames for different business entities in the automotive manufacturing domain.
It serves as the first step in the ETL pipeline, preparing raw data for subsequent
transformation and loading operations.

Key Features:
- Excel file reading and validation
- Creation of main DataFrame from source data
- Extraction of specialized DataFrames for different business domains
- Data validation and error handling
- Integration ready for Apache Airflow workflows

Main Components:
- create_main_dataframe(): Reads Excel file and creates primary DataFrame
- create_supplier_dataframe(): Extracts supplier-related data
- create_part_dataframe(): Extracts part master data
- create_box_dataframe(): Extracts box packaging specifications
- create_pallet_dataframe(): Extracts pallet packaging specifications
- create_model_dataframe(): Extracts vehicle model data
- create_workshop_dataframe(): Extracts production workshop data
- create_line_dataframe(): Extracts production line data
- extractor(): Main coordination function for data extraction

Data Entities Extracted:
- Supplier data: Company names, locations, localization status
- Part data: Part numbers, names, weights
- Packaging data: Box and pallet specifications, dimensions, capacities
- Production data: Models, workshops, assembly lines

Integration Notes:
- Designed for Airflow integration (commented decorators ready for activation)
- XCom push functionality prepared for task communication
- Configurable file path through Airflow DAG parameters
- Returns dictionary of DataFrames for downstream processing

Development Mode:
- Uses hardcoded file path for testing
- Comprehensive logging for debugging
- Standalone execution capability for development

Production Usage:
    from airflow.decorators import task
    
    @task(task_id="extractor_task")
    def extractor(**context):
        # Production implementation with Airflow context
        file_path = context["dag_run"].conf.get("file_path")
        # ... extraction logic
        context['ti'].xcom_push(key='df_dict', value=common_df_dict)

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engineering Center
Created: 2025
Last Modified: 2025
License: MIT
Status: Production

Uncomment "from airflow.decorators import task", 
"@task(task_id="extractor_task")", 
"file_path = context["dag_run"].conf.get("file_path")",
"context['ti'].xcom_push(key='df_dict', value=common_df_dict)",
add **context as argument in function extractor() after testing        
"""
from pathlib import Path
import os
import sys
import polars as pl
# from airflow.decorators import task

# The relative path to the root project directory
project_path = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(project_path) not in sys.path:
    sys.path.insert(0, str(project_path))

# Logger setup
from config import get_logger
logger = get_logger(__name__)

def create_supplier_dataframe(main_df) -> pl.DataFrame:
    '''
    Function creates a specialized DataFrame containing supplier information
    by extracting relevant columns from the main DataFrame.
    
    Arguments:
    - main_df: Primary DataFrame containing all source data
    
    Returns:
    - supplier_df: DataFrame with supplier-related columns
    
    Raises:
    - ValueError: If required columns are missing
    - TypeError: If main_df is not a DataFrame
    '''
    # List of required columns
    required_columns = [
        'SUPPLIER_NAME',
        'LOCATION',
        'CITY', 
        'STREET',
        'BUILDING',
        'LOCALIZATION'
    ]

    try:
        # Validate input
        if not isinstance(main_df, pl.DataFrame):
            error_msg = "main_df must be a polars DataFrame."
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Handle empty DataFrame
        if main_df.is_empty():
            logger.warning(
                "Main DataFrame is empty - creating empty supplier_df with expected structure."
            )
            return pl.DataFrame(schema={col: pl.String() for col in required_columns})

        # Check for required columns
        missing_columns = [col for col in required_columns if col not in main_df.columns]
        if missing_columns:
            error_msg = (
                f"Missing required columns for supplier data: {missing_columns}."
                f"Available columns: {list(main_df.columns)}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Extract supplier data
        supplier_df = main_df.select(required_columns)

        # Enhanced success logging
        logger.info(
            "Successfully created supplier DataFrame with %d rows and %d columns.",
            supplier_df.height,
            len(supplier_df.columns)
        )

        return supplier_df

    except (TypeError, ValueError):
        # Expected errors have already been logged, we are moving on.
        raise

    except Exception as e:
        # Catch any unexpected errors
        logger.error("Unexpected error creating supplier DataFrame: %s", e)
        raise

def create_part_dataframe(main_df) -> pl.DataFrame:
    '''
    Function creates a specialized DataFrame containing part master data
    by extracting relevant columns from the main DataFrame.
    
    Arguments:
    - main_df: Primary DataFrame containing all source data
    
    Returns:
    - part_df: DataFrame with part-related columns
    
    Raises:
    - ValueError: If required columns are missing
    - TypeError: If main_df is not a DataFrame
    '''
    # List of required columns
    required_columns = [
        'PART_NUMBER',
        'PART_NAME', 
        'PART_WEIGHT_KG'
    ]

    try:
        # Validate input
        if not isinstance(main_df, pl.DataFrame):
            error_msg = "main_df must be a polars DataFrame."
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Handle empty DataFrame
        if main_df.is_empty():
            logger.warning(
                "Main DataFrame is empty - creating empty part_df with expected structure."
            )
            return pl.DataFrame(schema={col: pl.String() for col in required_columns})

        # Check for required columns
        missing_columns = [col for col in required_columns if col not in main_df.columns]
        if missing_columns:
            error_msg = (
                f"Missing required columns for part data: {missing_columns}."
                f"Available columns: {list(main_df.columns)}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Extract part data
        part_df = main_df.select(required_columns)

        # Enhanced success logging
        logger.info(
            "Successfully created part DataFrame with %d rows and %d columns.",
            part_df.height,
            len(part_df.columns)
        )

        return part_df

    except (TypeError, ValueError):
        # Expected errors have already been logged, we are moving on.
        raise

    except Exception as e:
        # Catch any unexpected errors
        logger.error("Unexpected error creating part DataFrame: %s", e)
        raise

def create_box_dataframe(main_df) -> pl.DataFrame:
    '''
    Function creates a specialized DataFrame containing box packaging specifications
    by extracting relevant columns from the main DataFrame.
    
    Arguments:
    - main_df: Primary DataFrame containing all source data
    
    Returns:
    - box_df: DataFrame with box-related columns
    
    Raises:
    - ValueError: If required columns are missing
    - TypeError: If main_df is not a DataFrame
    '''
    # List of required columns
    required_columns = [
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

    try:
        # Validate input
        if not isinstance(main_df, pl.DataFrame):
            error_msg = "main_df must be a polars DataFrame."
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Handle empty DataFrame
        if main_df.is_empty():
            logger.warning(
                "Main DataFrame is empty - creating empty box_df with expected structure."
            )
            return pl.DataFrame(schema={col: pl.String() for col in required_columns})

        # Check for required columns
        missing_columns = [col for col in required_columns if col not in main_df.columns]
        if missing_columns:
            error_msg = (
                f"Missing required columns for box data: {missing_columns}."
                f"Available columns: {list(main_df.columns)}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Extract box data
        box_df = main_df.select(required_columns)

        # Enhanced success logging
        logger.info(
            "Successfully created box DataFrame with %d rows and %d columns.",
            box_df.height,
            len(box_df.columns)
        )

        return box_df

    except (TypeError, ValueError):
        # Expected errors have already been logged, we are moving on.
        raise

    except Exception as e:
        # Catch any unexpected errors
        logger.error("Unexpected error creating box DataFrame: %s", e)
        raise

def create_pallet_dataframe(main_df) -> pl.DataFrame:
    '''
    Function creates a specialized DataFrame containing pallet packaging specifications
    by extracting relevant columns from the main DataFrame.
    
    Arguments:
    - main_df: Primary DataFrame containing all source data
    
    Returns:
    - pallet_df: DataFrame with pallet-related columns
    
    Raises:
    - ValueError: If required columns are missing
    - TypeError: If main_df is not a DataFrame
    '''
    # List of required columns
    required_columns = [
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

    try:
        # Validate input
        if not isinstance(main_df, pl.DataFrame):
            error_msg = "main_df must be a polars DataFrame."
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Handle empty DataFrame
        if main_df.is_empty():
            logger.warning(
                "Main DataFrame is empty - creating empty pallet_df with expected structure.")
            return pl.DataFrame(schema={col: pl.String() for col in required_columns})

        # Check for required columns
        missing_columns = [col for col in required_columns if col not in main_df.columns]
        if missing_columns:
            error_msg = (
                f"Missing required columns for pallet data: {missing_columns}. "
                f"Available columns: {list(main_df.columns)}."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Extract pallet data
        pallet_df = main_df.select(required_columns)

        # Enhanced success logging with additional information
        logger.info(
            "Successfully created pallet DataFrame with %d rows and %d columns.",
            pallet_df.height,
            len(pallet_df.columns)
        )

        return pallet_df

    except (TypeError, ValueError):
        # Expected errors have already been logged, we are moving on.
        raise

    except Exception as e:
        # Catch any unexpected errors
        logger.error("Unexpected error creating pallet DataFrame: %s", e)
        raise

def create_model_dataframe(main_df) -> pl.DataFrame:
    '''
    Function creates a specialized DataFrame containing vehicle model information
    by extracting relevant columns from the main DataFrame.
    
    Arguments:
    - main_df: Primary DataFrame containing all source data
    
    Returns:
    - model_df: DataFrame with model-related columns
    
    Raises:
    - ValueError: If required columns are missing
    - TypeError: If main_df is not a DataFrame
    '''
    # List of required columns
    required_columns = [
        'MODEL_CODE',
        'MODEL_NAME'
    ]

    try:
        # Validate input
        if not isinstance(main_df, pl.DataFrame):
            error_msg = "main_df must be a polars DataFrame."
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Handle empty DataFrame
        if main_df.is_empty():
            logger.warning(
                "Main DataFrame is empty - creating empty model_df with expected structure"
            )
            return pl.DataFrame(schema={col: pl.String() for col in required_columns})

        # Check for required columns
        missing_columns = [col for col in required_columns if col not in main_df.columns]
        if missing_columns:
            error_msg = (
                f"Missing required columns for model data: {missing_columns}. "
                f"Available columns: {list(main_df.columns)}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Extract model data
        model_df = main_df.select(required_columns)

        # Enhanced success logging
        logger.info(
            "Successfully created model DataFrame with %d rows and %d columns.",
            model_df.height,
            len(model_df.columns)
        )

        return model_df

    except (TypeError, ValueError):
        # Expected errors have already been logged, we are moving on.
        raise

    except Exception as e:
        # Catch any unexpected errors
        logger.error("Unexpected error creating model DataFrame: %s", e)
        raise

def create_workshop_dataframe(main_df) -> pl.DataFrame:
    '''
    Function creates a specialized DataFrame containing production workshop information
    by extracting relevant columns from the main DataFrame.
    
    Arguments:
    - main_df: Primary DataFrame containing all source data
    
    Returns:
    - workshop_df: DataFrame with workshop-related columns
    
    Raises:
    - ValueError: If required columns are missing
    - TypeError: If main_df is not a DataFrame
    '''
    # List of required columns
    required_columns = [
        'WORKSHOP_CODE',
        'WORKSHOP_NAME'
    ]

    try:
        # Validate input
        if not isinstance(main_df, pl.DataFrame):
            error_msg = "main_df must be a polars DataFrame."
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Handle empty DataFrame
        if main_df.is_empty():
            logger.warning(
                "Main DataFrame is empty - creating empty workshop_df with expected structure."
            )
            return pl.DataFrame(schema={col: pl.String() for col in required_columns})

        # Check for required columns
        missing_columns = [col for col in required_columns if col not in main_df.columns]
        if missing_columns:
            error_msg = (
                f"Missing required columns for workshop data: {missing_columns}."
                f"Available columns: {list(main_df.columns)}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Extract workshop data
        workshop_df = main_df.select(required_columns)

        # Enhanced success logging with additional information
        logger.info(
            "Successfully created workshop DataFrame with %d rows and %d columns.",
            workshop_df.height,
            len(workshop_df.columns)
        )

        return workshop_df

    except (TypeError, ValueError):
        # Expected errors have already been logged, we are moving on.
        raise
    except Exception as e:
        # Catch any unexpected errors
        logger.error("Unexpected error creating workshop DataFrame: %s", e)
        raise

def create_line_dataframe(main_df) -> pl.DataFrame:
    '''
    Function creates a specialized DataFrame containing production line information
    by extracting relevant columns from the main DataFrame.
    
    Arguments:
    - main_df: Primary DataFrame containing all source data
    
    Returns:
    - line_df: DataFrame with production line-related columns
    
    Raises:
    - ValueError: If required columns are missing
    - TypeError: If main_df is not a DataFrame
    '''
    required_columns = [
        'LINE_CODE',
        'LINE_NAME'
    ]

    try:
        # Validate input
        if not isinstance(main_df, pl.DataFrame):
            error_msg = "main_df must be a polars DataFrame"
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Handle empty DataFrame
        if main_df.is_empty():
            logger.warning(
                "Main DataFrame is empty - creating empty line DataFrame with expected structure"
            )
            return pl.DataFrame(schema={col: pl.String() for col in required_columns})

        # Check for required columns
        missing_columns = [col for col in required_columns if col not in main_df.columns]
        if missing_columns:
            error_msg = (
                f"Missing required columns for production line data: {missing_columns}. "
                f"Available columns: {list(main_df.columns)}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Extract production line data
        line_df = main_df.select(required_columns)

        # Enhanced success logging with additional information
        logger.info(
            "Successfully created production line DataFrame with %d rows and %d columns.",
            line_df.height,
            len(line_df.columns)
        )

        return line_df

    except (TypeError, ValueError):
        # Ожидаемые ошибки - уже залогированы, пробрасываем дальше
        raise
    except Exception as e:
        # Непредвиденные ошибки - логируем и пробрасываем оригинальное исключение
        logger.error("Unexpected error creating production line DataFrame: %s", e)
        raise

# @task(task_id="extractor_task")
def extractor()-> dict[str, pl.DataFrame]:
    '''
    The main function that extracts data from Excel source file and creates multiple DataFrames
    for subsequent data cleaning and transformation steps. Reads source data and generates
    specialized DataFrames for different business entities.
    
    Arguments:
    - None: The function uses a predefined file path for data extraction
    
    Returns:
    - common_df_dict: Dictionary containing all extracted DataFrames with keys:
        * 'main_df': Primary DataFrame with all source data
        * 'supplier_df': Supplier information DataFrame
        * 'part_df': Part master data DataFrame
        * 'box_df': Box packaging specifications DataFrame
        * 'pallet_df': Pallet packaging specifications DataFrame
        * 'model_df': Vehicle model data DataFrame
        * 'workshop_df': Production workshop data DataFrame
        * 'line_df': Production line data DataFrame
    
    Source Data:
    - Reads from predefined Excel file path: sample_mft_data.xlsx
    - Validates file existence before processing
    - Uses main DataFrame as base for creating specialized DataFrames
    
    Data Processing Flow:
    1. Reads Excel file and creates main DataFrame
    2. Extracts supplier data from main DataFrame
    3. Extracts part master data from main DataFrame
    4. Extracts box packaging data from main DataFrame
    5. Extracts pallet packaging data from main DataFrame
    6. Extracts vehicle model data from main DataFrame
    7. Extracts workshop data from main DataFrame
    8. Extracts production line data from main DataFrame
    
    Note: Currently uses hardcoded file path for development purposes.
    In production, file path would be obtained from task configuration.
    '''

    # Get the file path from the task configuration
    # file_path = context["dag_run"].conf.get("file_path")
    # The relative path to the root project directory
    file_path = project_path / 'data/sample_mft_data.xlsx'

    # Check the file availability
    if not file_path or not os.path.exists(file_path):
        raise ValueError("The file was not found or the file path is missing.")

    main_df = None

    try:
        main_df = pl.read_excel(
            file_path,
            engine='openpyxl'
        )
    except Exception as e:
        logger.warning("Unexpected error reading file: %s.", e)

    common_df_dict = {
        'main_df': main_df,
        'supplier_df': create_supplier_dataframe(main_df),
        'part_df': create_part_dataframe(main_df),
        'box_df': create_box_dataframe(main_df),
        'pallet_df': create_pallet_dataframe(main_df),
        'model_df': create_model_dataframe(main_df),
        'workshop_df': create_workshop_dataframe(main_df),
        'line_df': create_line_dataframe(main_df)
    }

    logger.info("Successfully created common dictionary with %d DataFrames: %s",
        len(common_df_dict),
        ', '.join(list(common_df_dict.keys()))
    )

    # Push in XCom
    # context['ti'].xcom_push(key='df_dict', value=common_df_dict)
    return common_df_dict

if __name__ == '__main__':
    extractor()
