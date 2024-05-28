#!/usr/bin/env python3

import yaml
import cx_Oracle
import datetime
import sys
import subprocess
import csv
import os

CONFIG_FILE = sys.path[0] + "/googleSync.yaml"
OUTPUT_FILE = sys.path[0] + '/Last_GAM_Pull.csv'

# Load configuration
def load_config(config_file):
    with open(config_file, 'r') as config_in:
        return yaml.safe_load(config_in)

config = load_config(CONFIG_FILE)

# Print timestamped messages
def tprint(text):
    stamp = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f'{stamp}: {text}')

# Run GAM command to pull data
def gam_pull():
    gam_command = f"{config['gam']['path']} report users "
    
    if 'filters' in config['gam']:
        gam_command += f"filter '{','.join(config['gam']['filters'])}' "
    
    if 'mappings' in config['gam']:
        mappings_no_date = list(config['gam']['mappings'].keys())
        if 'date' in mappings_no_date:
            mappings_no_date.remove('date')
        gam_command += f"parameters '{','.join(sorted(mappings_no_date))}' "
    
    gam_command += f"> {OUTPUT_FILE}"
    
    tprint('Running GAM Report')
    subprocess.run(gam_command, shell=True, check=True)
    tprint('GAM Report Complete')

# Generate list of dictionaries from GAM output
def gen_gam_list_of_dicts():
    with open(OUTPUT_FILE) as f:
        gam_list_of_dicts = [{gam_mapper(k): str(v) for k, v in row.items()}
                             for row in csv.DictReader(f, skipinitialspace=True)]
    
    if not gam_list_of_dicts:
        sys.exit(f'No data in GAM output file: {OUTPUT_FILE}')
    
    return gam_list_of_dicts

# Map GAM keys based on config
def gam_mapper(val):
    return config['gam']['mappings'].get(val, val)

# Push data to Oracle database
def oracle_push(upsert_sql, data_to_upsert):
    conn_str = f"{config['oracle']['username']}/{config['oracle']['password']}@{config['oracle']['instance']}"
    conn = cx_Oracle.connect(conn_str)
    tprint(f'Connection to {config["oracle"]["instance"]} Successful')
    
    with conn.cursor() as cursor:
        if data_to_upsert:
            tprint('Starting SQL Execution')
            cursor.executemany(upsert_sql, data_to_upsert, batcherrors=True)
            for error in cursor.getbatcherrors():
                print("Error", error.message, "at row offset", error.offset)
            tprint('SQL Execution Finished')
    
    conn.commit()
    conn.close()

# Format SQL statements
def sql_formatting(bind, key):
    if 'data_formatting' in config['gam'] and key in config['gam']['data_formatting']:
        data_format = config['gam']['data_formatting'][key]
        if data_format == 'bool':
            return f"(CASE lower({bind}) WHEN 'true' THEN 'Y' ELSE 'N' END)"
        elif data_format == 'date_simple':
            return f"to_date({bind}, 'YYYY-MM-DD')"
        elif data_format == 'date_UTC':
            return f"cast(to_utc_timestamp_tz({bind}) as timestamp with local time zone)"
    return bind

# Generate SQL upsert statement
def gen_upsert_sql(sorted_gam_keys):
    primary_key = config['gam'].get('primary_key')
    if not primary_key:
        sys.exit('You must provide a primary key in the yaml to join correctly.')
    
    if primary_key not in sorted_gam_keys:
        sys.exit('Your primary key is invalid. Please make a mapping of (csv header) -> (DB primary key) in the yaml')
    
    keys_without_primary = sorted_gam_keys.copy()
    keys_without_primary.remove(primary_key)
    
    sql = (
        f"MERGE INTO google_accounts a USING (SELECT "
        f"{','.join([f'{sql_formatting(f':{i}', k)} {k}' for i, k in enumerate(sorted_gam_keys, start=1)])} "
        f"FROM dual) b ON (a.{primary_key} = b.{primary_key}) "
        f"WHEN MATCHED THEN UPDATE SET "
        f"{','.join([f'a.{k} = b.{k}' for k in keys_without_primary])} "
        f"WHEN NOT MATCHED THEN INSERT ({','.join([f'a.{k}' for k in sorted_gam_keys])}) "
        f"VALUES ({','.join([f'b.{k}' for k in sorted_gam_keys])})"
    )
    return sql

# Generate values for upsert statement
def gen_upsert_values(gam_dict, sorted_gam_keys):
    return [gam_dict[key] for key in sorted_gam_keys]

# Main execution
def main():
    gam_pull()
    gam_list_of_dicts = gen_gam_list_of_dicts()
    sorted_gam_keys = sorted(gam_list_of_dicts[0].keys())
    upsert_sql = gen_upsert_sql(sorted_gam_keys)
    
    data_to_upsert = [gen_upsert_values(gam_dict, sorted_gam_keys) for gam_dict in gam_list_of_dicts]
    
    oracle_push(upsert_sql, data_to_upsert)

if __name__ == '__main__':
    main()
