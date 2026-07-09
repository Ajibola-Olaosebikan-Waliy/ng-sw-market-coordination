"""
About the config.py:
1. It is the main configuration for the marketfed pipeline
2. It houses the parameters that influences the pipeline's behaviour
3. The file is located at src/pipeline/config.py
4. Parent: [0]=pipeline; [1]=src; [2]=project root
5. It has the data directories
6. It has the directory to the data source
7. It contains the selected target markets from the pool of food markets in the HDX database
8. It contains the list of selected products under consideration
"""
from pathlib import Path
## The Project's root
ROOT_DIR = Path(__file__).resolve().parents[2]

## Directories of Data
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR    = DATA_DIR / "processed"
QUALITY_DIR      = DATA_DIR / "quality_reports"


##____The HDX Data Source Settings___##
HDX_NIGERIA_URL = (
    "https://data.humdata.org/dataset/"
    "42db041f-7aaf-4ab4-961f-2a12096861e7/"
    "resource/12b51155-0cd3-4806-9924-61ede4077591/"
    "download/wfp_food_prices_nga.csv"
)

## Markets metadata URL — GPS coordinates per market
## Used for the coordination engine
HDX_MARKETS_URL = (
    "https://data.humdata.org/dataset/"
    "42db041f-7aaf-4ab4-961f-2a12096861e7/"
    "resource/5329e772-0b74-4f65-8cc0-37a0915cc7e4/"
    "download/wfp_markets_nga.csv"
)


##____Define Data Source Settings___##
#HDX_NIGERIA_URL = "https://humdata.org"



##___The target commodities___ (extracted from the list of food from the HDX database)
TARGET_COMMODITIES = ["Maize (white)", "Rice (imported)", "Millet"]

##___Category of Products___## 
TARGET_CATEGORY = "cereals and tubers"

##___ Target Country___##
TARGET_COUNTRY = "Nigeria"

##___Data Extraction Settings (it includes the max stream byte per load and max wait for HDX response)___##
DOWNLOAD_TIMEOUT_SECONDS = 120  
CHUNK_SIZE_BYTES         = 8192  
RAW_FILE_PREFIX          = "wfp_nga_cereals"

##___Market Registry: With market name as KEYS, and their supporting information as the VALUES___## 
MARKET_REGISTRY ={
    "Ibadan": {
        "client_id": 0,
        "state": "Oyo",
        "city": "Ibadan",
        "latitude": 7.3986,
        "longitude": 3.9022,
         "note":       "Proxy for Bodija Market in Ibadan — nearest available in WFP dataset",

    },
    "Lagos": {
        "client_id": 1,
        "state": "Lagos",
        "city": "Lagos",
        "latitude": 6.6018,
        "longitude": 3.3515,
        "note":       "Proxy for Mile 12 market in Lagos — nearest available in WFP dataset",
    },
    "Dawanau": {
        "client_id":  2,
        "state":      "Kano",
        "city":       "Kano",
        "latitude":   12.0022,
        "longitude":  8.5920,
        "note":       "Largest grain market in West Africa — strong price signal for comparison",
    },
}

TARGET_MARKETS  = list(MARKET_REGISTRY.keys())  ### The markets under consideration ###
NUM_FL_CLIENTS  = len(MARKET_REGISTRY)  ### The number of clients. Here, clients are the markets under consideration ###.

##__Partioning__##  (This is used to simulate the privacy boundary of true federated learning)
FEDERATED_CLIENTS_DIR = DATA_DIR / "clients"  

## Convenience storage map
## Groups all storage paths into a central folder for passing to functions
STORAGE_CONFIG = {
    "raw":       RAW_DIR,
    "clients":   FEDERATED_CLIENTS_DIR,
    "processed": PROCESSED_DIR,
    "quality":   QUALITY_DIR,
}

##___Dynamic Federated Client Storage Slugs___##
# Maps each market to its private, sandboxed partition data directory
CLIENT_PARTITION_PATHS = {
    market: FEDERATED_CLIENTS_DIR / market.lower().replace(" ", "_")
    for market in TARGET_MARKETS
}
