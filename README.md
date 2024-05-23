# DynoPreparare

DynoPreparare is an application designed to streamline the preparation process for inputting information into another application called DynoScript. DynoScript assists in generating SQL code for migrating data from one Salesforce Org to another. The primary purpose of DynoPreparare is to facilitate the data migration process by automating the extraction and analysis of field-level data from Salesforce Orgs.

## Usage

### Activating Virtual Environment

```bash
# On Windows
venv\Scripts\activate

# On Unix or MacOS
source venv/bin/activate
```


## Install Dependencies
```bash
pip install -r requirements.txt

```

## Run program
```bash
python3 scripts/main.py
```


## Dependencies
1. Python 3.x
2. Pandas
3. Requests
4. Supply Legacy and new Org credentials in scripts folder.
