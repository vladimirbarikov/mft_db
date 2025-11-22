-- Подключаемся к целевой базе данных
\c mft_db

-- Создаем тип локализации
CREATE TYPE localization AS ENUM ('yes', 'no');

-- Создаем тип упаковки
CREATE TYPE packaging_type AS ENUM ('returnable', 'non-returnable');

-- Создаем тип кодов моделей
CREATE TYPE model_codes AS ENUM ('A01', 'A08', 'B02', 'B04', 'B06', 'B16');

-- Создаем тип названий моделей
CREATE TYPE model_names AS ENUM ('Jolion', 'H3', 'F7', 'F7x', 'Dargo', 'H7');

-- Создаем тип кодов производственных цехов
CREATE TYPE workshop_codes AS ENUM ('AS', 'COMP', 'PAINT', 'WELD', 'STAMP', 'EN');

-- Создаем тип названий производственных цехов
CREATE TYPE workshop_names AS ENUM ('Assembly', 'Component', 'Painting', 'Welding', 'Stamping', 'Engine');

-- Таблица supplier_data
CREATE TABLE IF NOT EXISTS supplier_data (
    SUPPLIER_ID VARCHAR(10) NOT NULL PRIMARY KEY,
    SUPPLIER_NAME VARCHAR(200),
    LOCATION VARCHAR(50),
    CITY VARCHAR(50),
    STREET VARCHAR(100),
    BUILDING VARCHAR(10),
    LOCALIZATION localization
);

-- Таблица part_data
CREATE TABLE IF NOT EXISTS part_data (
    PART_ID VARCHAR(10) NOT NULL PRIMARY KEY,
    PART_NUMBER VARCHAR(50),
    PART_NAME VARCHAR(100),
    PART_WEIGHT_KG DECIMAL(5, 2),
    SUPPLIER_ID VARCHAR(10),
    FOREIGN KEY (SUPPLIER_ID) REFERENCES supplier_data (SUPPLIER_ID)
);

-- Таблица box_data
CREATE TABLE IF NOT EXISTS box_data (
    BOX_ID VARCHAR(10) NOT NULL PRIMARY KEY,
    BOX_NUMBER VARCHAR(50),
    BOX_TYPE packaging_type,
    BOX_WEIGHT_KG DECIMAL(5, 2),
    BOX_LENGTH_MM SMALLINT,
    BOX_WIDTH_MM SMALLINT,
    BOX_HEIGHT_MM SMALLINT,
    BOX_VOL_M3 DECIMAL(5, 2),
    BOX_AREA_M2 DECIMAL(5, 2),
    BOX_STACKING SMALLINT,
    CONSTRAINT chk_positive_box_volume_area CHECK (BOX_VOL_M3 >= 0 AND BOX_AREA_M2 >= 0)
);

-- Связующая таблица part_to_box
CREATE TABLE IF NOT EXISTS part_to_box (
    PART_ID VARCHAR(10) NOT NULL,
    BOX_ID VARCHAR(10) NOT NULL,
    PART_PER_BOX INT,
    PRIMARY KEY (PART_ID, BOX_ID),
    FOREIGN KEY (PART_ID) REFERENCES part_data (PART_ID),
    FOREIGN KEY (BOX_ID) REFERENCES box_data (BOX_ID)
);

-- Таблица pallet_data
CREATE TABLE IF NOT EXISTS pallet_data (
    PALLET_ID VARCHAR(10) NOT NULL PRIMARY KEY,
    PALLET_NUMBER VARCHAR(50),
    PALLET_TYPE packaging_type,
    PALLET_WEIGHT_KG DECIMAL(5, 2),
    PALLET_LENGTH_MM SMALLINT,
    PALLET_WIDTH_MM SMALLINT,
    PALLET_HEIGHT_MM SMALLINT,
    PALLET_VOL_M3 DECIMAL(5, 2),
    PALLET_AREA_M2 DECIMAL(5, 2),
    PALLET_STACKING SMALLINT,
    CONSTRAINT chk_positive_pallet_volume_area CHECK (PALLET_VOL_M3 >= 0 AND PALLET_AREA_M2 >= 0)
);

-- Связующая таблица box_to_pallet
CREATE TABLE IF NOT EXISTS box_to_pallet (
    BOX_ID VARCHAR(10) NOT NULL,
    PALLET_ID VARCHAR(10) NOT NULL,
    BOX_PER_PALLET SMALLINT,
    PRIMARY KEY (BOX_ID, PALLET_ID),
    FOREIGN KEY (BOX_ID) REFERENCES box_data (BOX_ID),
    FOREIGN KEY (PALLET_ID) REFERENCES pallet_data (PALLET_ID)
);

-- Таблица model_data
CREATE TABLE IF NOT EXISTS model_data (
    MODEL_ID VARCHAR(10) NOT NULL PRIMARY KEY,
    MODEL_CODE model_codes,
    MODEL_NAME model_names
);

-- Связующая таблица part_to_model
CREATE TABLE IF NOT EXISTS part_to_model (
    PART_ID VARCHAR(10) NOT NULL,
    MODEL_ID VARCHAR(10) NOT NULL,
    CONFIGURATION VARCHAR(20),
    PART_PER_VEHICLE SMALLINT,
    PRIMARY KEY (PART_ID, MODEL_ID),
    FOREIGN KEY (PART_ID) REFERENCES part_data (PART_ID),
    FOREIGN KEY (MODEL_ID) REFERENCES model_data (MODEL_ID)
);

-- Таблица workshop_data
CREATE TABLE IF NOT EXISTS workshop_data (
    WORKSHOP_ID VARCHAR(10) NOT NULL PRIMARY KEY,
    WORKSHOP_CODE workshop_codes,
    WORKSHOP_NAME workshop_names
);

-- Таблица line_data
CREATE TABLE IF NOT EXISTS line_data (
    LINE_ID VARCHAR(10) NOT NULL PRIMARY KEY,
    LINE_CODE VARCHAR(10),
    LINE_NAME VARCHAR(50),
    WORKSHOP_ID VARCHAR(10),
    FOREIGN KEY (WORKSHOP_ID) REFERENCES workshop_data (WORKSHOP_ID)
);

-- Связующая таблица part_to_station
CREATE TABLE IF NOT EXISTS part_to_line (
    PART_ID VARCHAR(10) NOT NULL,
    LINE_ID VARCHAR(10) NOT NULL,
    PRIMARY KEY (PART_ID, LINE_ID),
    FOREIGN KEY (PART_ID) REFERENCES part_data (PART_ID),
    FOREIGN KEY (LINE_ID) REFERENCES line_data (LINE_ID)
);