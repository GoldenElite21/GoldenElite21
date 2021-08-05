#!/usr/bin/python3

import yaml
import cx_Oracle
import datetime
import sys
import subprocess
import csv


with open(sys.path[0] + "/googleSync.yaml", 'r') as config_in:
     config = yaml.safe_load(config_in)

gam = config['gam']
output = sys.path[0] + '/Last_GAM_Pull.csv'

##--------------- Helpers ---------------
def tprint(text):
  stamp = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
  print(stamp + ': ' + text)

##--------------- GAM ---------------
def gamPull():
  gamCommand = gam['path'] + " report users "
  if('filters' in gam.keys() and gam['filters']):
    gamCommand += "filter '" + ",".join(gam['filters']) + "' "
  if('mappings' in gam.keys() and gam['mappings']):
    mappingsNoDate = list(gam['mappings'].keys())
    mappingsNoDate.remove('date')
    gamCommand += "parameters '" +  ",".join(sorted(mappingsNoDate)) + "' "
  gamCommand += "> " + output
  tprint('Running GAM Report')
  gamProcess = subprocess.Popen(gamCommand, shell=True)
  gamProcess.wait()
  tprint('GAM Report Complete')


def genGAMListOfDicts():
  with open(output) as f: #python 3.6+ dicts are insertion ordered
    gamListOfDicts = [{gamMapper(k): str(v) for k, v in row.items()}
                     for row in csv.DictReader(f, skipinitialspace=True)]

  if not gamListOfDicts:
    sys.exit('No data in GAM output file: ' + output)

  return gamListOfDicts

def gamMapper(val):
  if('mappings' in gam.keys() and gam['mappings']) and (val in gam['mappings'].keys() and gam['mappings'][val]):
    return gam['mappings'][val]
  else:
    return val

##--------------- Banner ---------------
def oraclePush(upsertSQL, dataToUpsert):
  username = config['oracle']['username']
  password = config['oracle']['password']
  instance = config['oracle']['instance']

  conn_str = username + u'/' + password + u'@' + instance
  connOracle = cx_Oracle.connect(conn_str)
  tprint('Connection to ' + instance + ' Successful')
  c = connOracle.cursor()

  if dataToUpsert:
    tprint('Starting SQL Execution')
    c.executemany(upsertSQL, dataToUpsert, batcherrors=True)
    for error in c.getbatcherrors():
      print("Error", error.message, "at row offset", error.offset)
    tprint('SQL Execution Finished')

  connOracle.commit()
  connOracle.close()


def sqlFormatting(bind, key):
  ##Make sure data_formatting is in the .yaml
  if ('data_formatting' in gam.keys() and gam['data_formatting'] 
    and key in gam['data_formatting'] and gam['data_formatting'][key]): 
    data_format = gam['data_formatting'][key]
    ##Format the statement if mapping exists
    if(data_format == 'bool'):
      return '(CASE lower(' + bind + ") WHEN 'true' THEN 'Y' ELSE 'N' END)"
    elif(data_format == 'date_simple'):
      return 'to_date(' + bind + ", 'YYYY-MM-DD')"
    elif(data_format == 'date_UTC'):
      return 'cast(to_utc_timestamp_tz(' + bind + ') as timestamp with local time zone)'

  return bind

def genUpsertSQL(sortedGAMKeys):

  keysWithoutPrimary = sortedGAMKeys.copy()
  if not ('primary_key' in gam.keys() and gam['primary_key']):
    sys.exit('You must provide a primary key in the yaml to join correctly.')

  if not (gam['primary_key'] in sortedGAMKeys):
    sys.exit('Your primary key is invalid. Please make a mapping of (csv header) -> (DB primary key) in the yaml')

  keysWithoutPrimary.remove(gam['primary_key'])

  ##Oracle Upsert: https://stackoverflow.com/questions/31579163/upsert-into-oracle
  sql = ("MERGE INTO google_accounts a USING (SELECT "
    + (",".join(['\n' + sqlFormatting(f':{bindNum}',f'{key}') + ' ' + key for bindNum, key in enumerate(sortedGAMKeys, start=1)]))
    + "\nFROM dual) b ON (a." + gam['primary_key'] + ' = b.' + gam['primary_key'] + ')'
    + "\n WHEN matched THEN UPDATE SET "
    + (",".join([f'\na.{key} = b.{key}' for key in keysWithoutPrimary]))
    + "\n WHEN NOT matched THEN INSERT ("
    + (",".join([f'\na.{key}' for key in sortedGAMKeys]))
    + "\n) VALUES ("
    + (",".join([f'\nb.{key}' for key in sortedGAMKeys]))
    + "\n)"
  )

  return sql

def genUpsertValues(gamDict, sortedGAMKeys):
  #doing in this way to retain list order regardless of insert order changes
  upsertValues = []
  for key in sortedGAMKeys:
    upsertValues.append(gamDict[key])

  return upsertValues

##--------------- Main ---------------
gamPull()
gamListOfDicts = genGAMListOfDicts()
sortedGAMKeys = sorted(gamListOfDicts[0].keys())
upsertSQL = genUpsertSQL(sortedGAMKeys)
dataToUpsert = []
for gamDict in gamListOfDicts:
  upsertValues = genUpsertValues(gamDict, sortedGAMKeys)
  if upsertValues:
    dataToUpsert.append(upsertValues)
oraclePush(upsertSQL, dataToUpsert)

