import subprocess
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
import json


def run_shell(cmd):

    response = subprocess.Popen(cmd,  shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.read()
    return get_shell_response(response)


def get_shell_response(data):

    response = data.decode('utf8').replace('\r', '')
    response = [resp.split(',') for resp in response.split('\n')]
    response = [fix_numeric_values(resp) for resp in response]
    return response


def fix_numeric_values(_list):

    for value in _list:
        index = _list.index(value)
        if isinstance(value, str):
            if value.isdigit():
                _list[index] = float(value)
            elif value == 'None':
                _list[index] = 0
            else:
                try:
                    _list[index] = float(value)
                except:
                    pass

        elif value == None:
            _list[index] == 0

    return _list


def save_log(error):

    if error == 'OK':
        msg = 'Code executed succefully'
    
    elif isinstance(error, ConnectionRefusedError):
        msg = 'Execution error: Connection refused'

    elif isinstance(error, Exception):
        msg = 'An unexpected error ocurred: {}'.format(error)

    else:
        msg = 'Execution error: Function has recieved an unexpected argument'

    today = datetime.today()
    today_str = '{}/{}/{}'.format(today.day, today.month, today.year)
    with open('log.txt', 'a') as f:
        f.write('{} - {}\n'.format(today_str, msg))


def run_bat():

    def get_main_list():
    
        cmd = 'kaggle competitions list --group entered --csv'
        response = run_shell(cmd)
        response = pd.DataFrame(data=response[1:-1], columns=response[0])
        return json.loads(response.to_json(orient='table'))['data']

    def fix_found_data(_list):

        for doc in _list:
            get_more_data(doc)

    def get_more_data(doc):

        ref = doc['ref']
        
        def get_submissions():

            cmd = 'kaggle competitions submissions {} --csv'.format(ref)
            response = run_shell(cmd)
            response = pd.DataFrame(data=response[1:-1], columns=response[0])
            if response.shape[0] > 0:
                response.sort_values(by=['publicScore'], inplace=True)
            return json.loads(response.to_json(orient='table'))['data']
        
        def get_position():
            
            # This is complex since kaggle does not return this info,
            # I have to donwload the whole leaderboard for each competition,
            # then compare with my submissions to find my position
            cmd = 'kaggle competitions leaderboard {} --download'.format(ref)
            response = run_shell(cmd)
            return 0

        doc['submissions'] = get_submissions()
        doc['submissionCount'] = len(doc['submissions'])
        doc['friendlyRef'] = doc['ref'].replace('-', ' ').title()
        if doc['submissionCount'] > 0:
            doc['higherScore'] = float(doc['submissions'][-1]['publicScore'])
        else:
            doc['higherScore'] = None

    main_list = get_main_list()
    fix_found_data(main_list)

    return main_list
    

def update_db(doc_list, db):

    db.competitions.delete_many({})
    db.competitions.insert_many(doc_list)


def connect():

    with open('C://Users//Yan//Desktop//Scripting//doenteMental.py//kaggle scrapper//mongo_str.txt', 'r') as f:
        mongo_str = f.read()
        
    client = MongoClient(mongo_str)
    db = client.kaggle
    return db


def run():
    
    run_result = 'OK'
    try:
        db = connect()
        doc_list = run_bat()
        update_db(doc_list, db)
    except Exception as error:
        run_result = error

    save_log(run_result)


run()


