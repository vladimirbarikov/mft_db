-- Connect to target database
\c mft_db

-- Create localization type
CREATE TYPE localization AS ENUM ('yes', 'no');

-- Create packaging type
CREATE TYPE packaging_type AS ENUM ('returnable', 'non-returnable');

-- Create model codes type
CREATE TYPE model_codes AS ENUM ('A01', 'A08', 'B02', 'B04', 'B06', 'B16');

-- Create model names type
CREATE TYPE model_names AS ENUM ('Jolion', 'H3', 'F7', 'F7x', 'Dargo', 'H7');

-- Create workshop codes type
CREATE TYPE workshop_codes AS ENUM ('AS', 'COMP', 'PAINT', 'WELD', 'STAMP', 'EN');

-- Create workshop names type
CREATE TYPE workshop_names AS ENUM ('Assembly', 'Component', 'Painting', 'Welding', 'Stamping', 'Engine');

-- Table supplier_data
CREATE TABLE IF NOT EXISTS supplier_data (
    SUPPLIER_ID VARCHAR(12) NOT NULL PRIMARY KEY,
    SUPPLIER_NAME VARCHAR(200),
    LOCATION VARCHAR(50),
    CITY VARCHAR(50),
    STREET VARCHAR(100),
    BUILDING VARCHAR(10),
    LOCALIZATION localization
);

-- Table part_data
CREATE TABLE IF NOT EXISTS part_data (
    PART_ID VARCHAR(12) NOT NULL PRIMARY KEY,
    PART_NUMBER VARCHAR(50),
    PART_NAME VARCHAR(100),
    PART_WEIGHT_KG DECIMAL(5, 2),
    SUPPLIER_ID VARCHAR(10),
    FOREIGN KEY (SUPPLIER_ID) REFERENCES supplier_data (SUPPLIER_ID)
);

-- Table box_data
CREATE TABLE IF NOT EXISTS box_data (
    BOX_ID VARCHAR(12) NOT NULL PRIMARY KEY,
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

-- Link table part_to_box
CREATE TABLE IF NOT EXISTS part_to_box (
    PART_ID VARCHAR(12) NOT NULL,
    BOX_ID VARCHAR(12) NOT NULL,
    PART_PER_BOX INT,
    PRIMARY KEY (PART_ID, BOX_ID),
    FOREIGN KEY (PART_ID) REFERENCES part_data (PART_ID),
    FOREIGN KEY (BOX_ID) REFERENCES box_data (BOX_ID)
);

-- Table pallet_data
CREATE TABLE IF NOT EXISTS pallet_data (
    PALLET_ID VARCHAR(12) NOT NULL PRIMARY KEY,
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

-- Link table box_to_pallet
CREATE TABLE IF NOT EXISTS box_to_pallet (
    BOX_ID VARCHAR(12) NOT NULL,
    PALLET_ID VARCHAR(12) NOT NULL,
    BOX_PER_PALLET SMALLINT,
    PRIMARY KEY (BOX_ID, PALLET_ID),
    FOREIGN KEY (BOX_ID) REFERENCES box_data (BOX_ID),
    FOREIGN KEY (PALLET_ID) REFERENCES pallet_data (PALLET_ID)
);

-- Table model_data
CREATE TABLE IF NOT EXISTS model_data (
    MODEL_ID VARCHAR(12) NOT NULL PRIMARY KEY,
    MODEL_CODE model_codes,
    MODEL_NAME model_names
);

-- Link table part_to_model
CREATE TABLE IF NOT EXISTS part_to_model (
    PART_ID VARCHAR(12) NOT NULL,
    MODEL_ID VARCHAR(12) NOT NULL,
    CONFIGURATION VARCHAR(20),
    PART_PER_VEHICLE SMALLINT,
    PRIMARY KEY (PART_ID, MODEL_ID),
    FOREIGN KEY (PART_ID) REFERENCES part_data (PART_ID),
    FOREIGN KEY (MODEL_ID) REFERENCES model_data (MODEL_ID)
);

-- Table workshop_data
CREATE TABLE IF NOT EXISTS workshop_data (
    WORKSHOP_ID VARCHAR(12) NOT NULL PRIMARY KEY,
    WORKSHOP_CODE workshop_codes,
    WORKSHOP_NAME workshop_names
);

-- Table line_data
CREATE TABLE IF NOT EXISTS line_data (
    LINE_ID VARCHAR(12) NOT NULL PRIMARY KEY,
    LINE_CODE VARCHAR(10),
    LINE_NAME VARCHAR(50),
    WORKSHOP_ID VARCHAR(10),
    FOREIGN KEY (WORKSHOP_ID) REFERENCES workshop_data (WORKSHOP_ID)
);

-- Link table part_to_line
CREATE TABLE IF NOT EXISTS part_to_line (
    PART_ID VARCHAR(12) NOT NULL,
    LINE_ID VARCHAR(12) NOT NULL,
    PRIMARY KEY (PART_ID, LINE_ID),
    FOREIGN KEY (PART_ID) REFERENCES part_data (PART_ID),
    FOREIGN KEY (LINE_ID) REFERENCES line_data (LINE_ID)
);

-- Table breakpoint_data
CREATE TABLE IF NOT EXISTS breakpoint_data (
    BREAKPOINT_ID VARCHAR(12) NOT NULL PRIMARY KEY,
    INPUT_DATE TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    BREAKPOINT_NUMBER VARCHAR(10) NOT NULL,
    BREAKPOINT_DATE TIMESTAMP WITH TIME ZONE
);

-- Link table part_to_breakpoint
CREATE TABLE IF NOT EXISTS part_to_breakpoint (
    PART_ID VARCHAR(12) NOT NULL,
    BREAKPOINT_ID VARCHAR(12) NOT NULL
    PART_NUMBER_BEFORE_CHANGE VARCHAR(50),
    SUPPLIER_NAME_BEFORE_CHANGE VARCHAR(200),
    LOCALIZATION_BEFORE_CHANGE localization,
    LINE_NAME_BEFORE_CHANGE VARCHAR(50),
    PRIMARY KEY (PART_ID, BREAKPOINT_ID),
    FOREIGN KEY (PART_ID) REFERENCES part_data (PART_ID),
    FOREIGN KEY (BREAKPOINT_ID) REFERENCES breakpoint_data (BREAKPOINT_ID)
);

