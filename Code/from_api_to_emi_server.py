import requests
import pandas as pd
import math
import psycopg2
import numpy as np


# Extraction des données de l'API dans un DataFrame


# BY QUART D'HEURE
def extract_conso_quarth(n_days = 3, days_delay = 0):
    """
    Extracts consumption data from the eco2mix API for the PACA Metropoles .
    For each 15 minutes.

    Args:
    - n_days (int): Number of days to include in the export. Default is set to 3.
    - days_delay (int): Delay in days between now and the most recent day included. Default is set to 1 to get data from the previous day. 0 would give missing consumption values

    Returns:
    - DataFrame: DataFrame containing the extracted data, the consumption at each 1/4 hour for the specified days.
    """
    # API endpoint
    url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-metropoles-tr/records"

    # Requests parameters
    offset = 3 * 96 * days_delay   # because 1 day 1 city is 96 rows
    params = {
        "where": 'libelle_metropole in ("Métropole Toulon-Provence-Méditerranée", "Métropole Nice Côte d\'Azur", "Métropole d\'Aix-Marseille-Provence")',
        "order_by": "date_heure DESC",
        "limit": 96,  # Limit of rows, max is 100. 96 is the number of rows for 1 city for 1 day
        "offset": offset     # Offset = 96 * 3 metropoles = 288, since we are fetching data for the previous day
    }

    # Data storage
    all_records = []
    n_requests = n_days * 3  # 3 requests of 96 rows = 1 day

    for _ in range(n_requests):                     # 10 requests
        response = requests.get(url, params=params)
        if response.status_code == 200:     # Success
            data = response.json()
            records = data['results']
            all_records.extend(records)     # Add this request to the results
            
            params["offset"] += params["limit"]    # Shift for the next request
        else:
            print("Échec de la requête :", response.status_code)
            break

    # Create DataFrame from the collected records
    df = pd.DataFrame(all_records)
    df.drop(["nature", "production", "echanges_physiques"], axis=1, inplace=True)
    # Rename columns for clarity
    df.rename(columns={"libelle_metropole": "Libelle_metropole", "date_heure":"Date_heure", "consommation": "Consommation_MW", "heures":"Heure", "date":"Date"}, inplace=True)
    # date & heure corrected
    df['Date_heure'] = df['Date'] + ' ' + df['Heure']
    df['Date_heure'] = pd.to_datetime(df['Date_heure'])
    df["Date"] = df.Date_heure.dt.date
    df["Heure"] = df.Date_heure.dt.time
    df["Heure"] = pd.to_datetime(df['Heure'], format='%H:%M:%S').dt.strftime('%H:%M')
    # df = df.fillna(0)
    df = df[df["Consommation_MW"].notna()].reset_index(drop=True)
    # Formats
    df["Heure"] = df["Heure"].apply(lambda x: str(x))
    df['Date_heure'] = df['Date_heure'].apply(lambda x: str(x))
    df["Consommation_MW"] = df["Consommation_MW"].apply(lambda x: str(int(x)))
    
    
    # reorga
    # df = df.pivot_table(index = "Date_heure", columns = "Libelle_metropole", values = "Consommation").reset_index()
    # df.columns.name = None
    # df.sort_values(by = "Date_heure", ascending = False, inplace = True, ignore_index=True)
    # df.rename(columns = {"Métropole Nice Côte d'Azur": "Consommation_Nice", "Métropole Toulon-Provence-Méditerranée": "Consommation_Toulon"}, inplace=True)
    # df["Date"] = df.Date_heure.dt.date
    # df["Heure"] = df.Date_heure.dt.time
    # df["Heure"] = pd.to_datetime(df['Heure'], format='%H:%M:%S').dt.strftime('%H:%M')
    
    return df










# BY DAY

def request_groupby(n_days, days_delay):
    # API endpoint
    url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-metropoles-tr/records"

    # Requests parameters
    params = {
        "select": "date, libelle_metropole, sum(consommation) / 4",
        "where": 'libelle_metropole in ("Métropole Toulon-Provence-Méditerranée", "Métropole Nice Côte d\'Azur", "Métropole d\'Aix-Marseille-Provence")',
        "group_by": "date, libelle_metropole",
        "order_by": "date DESC",
        "limit": 100,  # Limit of rows, max is 100
        "offset": 3 * days_delay     # delay * number of cities
    }

    # Data storage
    all_records = []
    n_requests = math.ceil(n_days / params["limit"]) * 3  # number of requests needed to have at least n_days. Example : 365 days -> (4 requests of 100) * 2 cities

    for _ in range(n_requests):    
        response = requests.get(url, params=params)
        if response.status_code == 200:     # Success
            data = response.json()
            records = data['results']
            all_records.extend(records)     # Add this request to the results
            
            params["offset"] += params["limit"]    # Shift for the next request
        else:
            print("Échec de la requête :", response.status_code)
            break

    # Create DataFrame from the collected records
    df = pd.DataFrame(all_records)
    df = df[0 : n_days*3]   # to get precisely n_days days
    df = df.rename(columns = {"date": "Date", "libelle_metropole":"Libelle_metropole", "sum(consommation) / 4":"Consommation_MWh"})
    #types
    df["Date"] = pd.to_datetime(df["Date"])
    
    return df

def request_groupby_count_miss(n_days, days_delay):
    # API endpoint
    url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-metropoles-tr/records"

    # Requests parameters
    params = {
        "select": "date, libelle_metropole, 100 - count(consommation)/0.96 as missing_percentage",         # Donnée valide : Supérieure à 50 (arbitraire)
        "where": 'libelle_metropole in ("Métropole Toulon-Provence-Méditerranée", "Métropole Nice Côte d\'Azur", "Métropole d\'Aix-Marseille-Provence")',
        "group_by": "date, libelle_metropole",
        "order_by": "date DESC",
        "limit": 100,  # Limit of rows, max is 100
        "offset": 3 * days_delay     # Delay * number of cities
    }

    # Data storage
    all_records = []
    n_requests = math.ceil(n_days / params["limit"]) * 3  # number of requests needed to have at least n_days. Example : 365 days -> (4 requests of 100) * 3 cities

    for _ in range(n_requests):    
        response = requests.get(url, params=params)
        if response.status_code == 200:     # Success
            data = response.json()
            records = data['results']
            all_records.extend(records)     # Add this request to the results
            
            params["offset"] += params["limit"]    # Shift for the next request
        else:
            print("Échec de la requête :", response.status_code)
            break

    # Create DataFrame from the collected records
    df = pd.DataFrame(all_records)
    df = df[0 : n_days*3]   # to get precisely n_days days
    df = df.rename(columns = {"date": "Date", "libelle_metropole":"Libelle_metropole"})
    #types
    df["Date"] = pd.to_datetime(df["Date"])
    df["missing_percentage"] = np.round(df["missing_percentage"], 2)
    
    return df
    

def extract_conso_jour(n_days = 20, days_delay = 0):
    """
    Extracts consumption data from the eco2mix API for the Metropoles Toulon-Provence-Méditerranée and Nice Côte d'Azur.
    For each day.

    Args:
    - n_days (int): Number of days to include in the export. Default is set to 20.
    - days_delay (int): Number of (most recent) days to skip. Put 1 to skip today and avoid missing values. Default = 0.
    
    Returns:
    - DataFrame: DataFrame containing the extracted data, the consumption of the 2 cities for each day.
    """
    df = request_groupby(n_days, days_delay)    # conso by date and city
    df_count = request_groupby_count_miss(n_days, days_delay)   # % of missing values by date and city
    df = pd.merge(left = df, right = df_count)  # adding the count of missing values into df
    
    # final types str, to export in sql
    df["Consommation_MWh"] = df["Consommation_MWh"].apply(lambda x: str(int(x)))
    df["missing_percentage"] = df["missing_percentage"].apply(lambda x: str(x))
        
    return df








# UPDATE IN SQL

def maj_data_conso(n_days=3, days_delay=0, timing="quart"):
    """
    Updates the consumption data from Eco2Mix into the right PostrgeSQL table of src_res_tps_reel.

    Args:
    - n_days (int): Number of days included in the new table. 
    - days_delay (int): Number of (most recent) days to skip. Put 1 to skip today and avoid missing values. Default = 0.
    - timing (string): The timing step you work with. Impacts which table is updated. Can be "quart" or "jour". Defaults to "quart".

    
    Returns:
    - None
    """
    # connexion to database inv
    conn = psycopg2.connect("host=172.16.13.168 dbname=inv user=emi") # Em1ss!
    cursor = conn.cursor()
    
    if timing == "quart":
        data = extract_conso_quarth(n_days = n_days, days_delay = days_delay)
        
        # INSERTION INTO EXISTING HISTORIC
        try:
            # creation if new
            cursor.execute("""
                    CREATE TABLE IF NOT EXISTS src_res_tps_reel.src_conso_elec_4_test (
                        Date_heure TIMESTAMP,
                        Date DATE, 
                        Heure VARCHAR,
                        code_insee_epci INT,
                        Libelle_metropole VARCHAR,
                        Consommation_MW INT,
                        PRIMARY KEY (date_heure, libelle_metropole)
                        );""")
            rows_inserted = 0
            rows_updated = 0
            for i in range(len(data)):  # insertion of every row
                date_heure = data["Date_heure"][i]
                date = data["Date"][i]
                heure = data["Heure"][i]
                code_insee = data["code_insee_epci"][i]
                ville = data["Libelle_metropole"][i]
                conso = data["Consommation_MW"][i]
                
                # What is the corresponding value already existing (can be None if new lign) :
                cursor.execute('''
                    SELECT Consommation_MW FROM src_res_tps_reel.src_conso_elec_4_test
                    WHERE Date_heure = %s AND Libelle_metropole = %s;
                ''', (date_heure, ville))
                existing_conso = cursor.fetchone()
                
                if existing_conso is not None and existing_conso[0] != int(conso):  # IF EXISTS AND NEW CONSO DIFFERENT, UPDATE
                    cursor.execute('''
                        UPDATE src_res_tps_reel.src_conso_elec_4_test
                        SET Consommation_MW = %s
                        WHERE Date_heure = %s AND Libelle_metropole = %s;
                    ''', (conso, date, ville))
                    rows_updated += 1  # count
                elif existing_conso is None:                  # IF NO VALUE, INSERTION
                    cursor.execute('''
                        INSERT INTO src_res_tps_reel.src_conso_elec_4_test (Date_heure, Date, Heure, code_insee_epci, Libelle_metropole, Consommation_MW)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    ''', (date_heure, date, heure, code_insee, ville, conso))
                    rows_inserted += 1  # count
                conn.commit()
                
            # Trier la table
            cursor.execute("SELECT * INTO src_res_tps_reel.src_conso_elec_4_test_sorted FROM src_res_tps_reel.src_conso_elec_4_test ORDER BY Date_heure DESC, libelle_metropole;")
            cursor.execute("DROP TABLE IF EXISTS src_res_tps_reel.src_conso_elec_4_test;")
            cursor.execute("ALTER TABLE src_res_tps_reel.src_conso_elec_4_test_sorted RENAME TO src_conso_elec_4_test;") 
            cursor.execute("COMMENT ON TABLE src_res_tps_reel.src_conso_elec_4_test IS 'Données de consommation électrique quart-horaires à Nice, Aix-Marseille, et Toulon';")
            conn.commit()
            cursor.close()  
            print(f'{rows_inserted} lignes insérées et {rows_updated} lignes mises à jour dans src_res_tps_reel.src_conso_elec_4_test')
        except IOError as io:
            print("erreur")     
    
    
    
    elif timing == "jour":
        data = extract_conso_jour(n_days = n_days, days_delay = days_delay)

         # RECHARGING ALL
        try:
            # creation of a table
            cursor.execute("""
                    drop table IF EXISTS src_res_tps_reel.src_conso_elec_jour_test_hist;
                    CREATE TABLE src_res_tps_reel.src_conso_elec_jour_test_hist (
                        Date DATE,
                        Libelle_metropole VARCHAR,
                        id_metropole INT,
                        Consommation_MWh INT,
                        missing_percentage FLOAT
                    );""")
            for i in range(len(data)):  # insertion of every row
                date = data["Date"][i]
                ville = data["Libelle_metropole"][i]
                conso = data["Consommation_MWh"][i]
                miss = data["missing_percentage"][i]
                cursor.execute('''
                    INSERT INTO src_res_tps_reel.src_conso_elec_jour_test_hist (Date, Libelle_metropole, Consommation_MWh, missing_percentage)
                    VALUES (%s, %s, %s, %s);
                ''', (date, ville, conso, miss))
                cursor.execute("""
                               UPDATE src_res_tps_reel.src_conso_elec_jour_test_hist
                                SET id_metropole = CASE 
                                    WHEN libelle_metropole LIKE 'Métropole Nice%' THEN 6100
                                    WHEN libelle_metropole LIKE 'Métropole Toulon%' THEN 83100
                                    WHEN libelle_metropole LIKE '%Aix-Marseille-Provence' THEN 13000
                                    ELSE NULL
                                END;""")
                conn.commit()
            cursor.close()  
            print(f'{len(data)} lignes chargées dans src_res_tps_reel.src_conso_elec_jour_test_hist')
        except IOError as io:
            print("erreur")                 
            
            
# USE 
# maj_data_conso(n_days = 2000, days_delay=0, timing="jour")
maj_data_conso(n_days = 15)