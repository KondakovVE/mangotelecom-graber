import requests
import urllib
import datetime
import time
import string
import json
from hashlib import sha256
import pandas
from io import StringIO
import re
import os
import sys
import base64
import credentals

df = pandas.DataFrame()

def get_variables(): 
    global params
    params = credentals.credentals()
    params["url"]       = "https://app.mango-office.ru/vpbx/"
    params["headers"]   = {"Content-Type":"application/x-www-form-urlencoded"}
    params["date_from"] = datetime.datetime(2020,3,20)
    params["date_to"]   = datetime.datetime.now()
    params["current_dir"]  = os.path.curdir
    params["tmp_file"]  = params["current_dir"] + '/mango_tmp.csv'

def mango_call(query, end_point):
    presign = (params["ApiKey"] + query + params["ApiSalt"]).encode("utf-8")
    sign = sha256(presign).hexdigest()
    query = urllib.request.quote(query.encode("utf-8"))
    body = "vpbx_api_key="+ params["ApiKey"] + "&sign=" + sign + "&json=" + query
    
    r = requests.post(params["url"]+end_point, headers=params["headers"], data=body)
    return r

def request_calls(date_from, date_to):

    json_query = '{  \
                "date_from": ' +  str(int(date_from.timestamp()))  + ',  \
                "date_to": ' + str(int(date_to.timestamp())) +',    \
                "fields": "records, entry_id, start, finish, from_extension, from_number, to_extension, to_number, disconnect_reason\"  \
            }'.translate(string.whitespace)
    r = mango_call(json_query, "stats/request")    
    if (not re.match('{".*":".*"}',r.text)):
        raise ValueError('def request_calls. Problem with URL response, its not matched with regular expression')
    return r.text 

def request_callback(callback):  
 
    for _ in range(10):
        r = mango_call(callback, "stats/result")    
        if r.text == "":
            time.sleep(5)
        else:
            break
    if len(r.text[:1000]) != 1000: 
        raise ValueError('request_callback(). Request text is not correct or empty.You can try to run script later. For period of start_date: '+ str(params["date_from"]) )
        
    return r.text


def range_dates(date_from,date_to):
    range_list = []
  
    while date_from < date_to:
        range_list.append([date_from,date_from +  datetime.timedelta(days=10)])
        date_from = date_from +  datetime.timedelta(days=10)
    
    return range_list

def text_to_df(text):

    def evlal_duration(s,f):
        return (f-s).seconds
    
    def eval_records(r):
        return r[1:-1].split(',')

    df = pandas.read_csv(StringIO(text)
                    ,header=0
                    ,delimiter=";"
                    ,names=["records", "entry_id", "start", "finish", "from_extension", "from_number", "to_extension", "to_number", "disconnect_reason",])

    df['start'] = pandas.to_datetime(df['start'],unit='s')
    df['finish'] = pandas.to_datetime(df['finish'],unit='s')
    df['records'] = df.apply(lambda x: eval_records(x['records']),axis=1)
    df['duration'] = df.apply(lambda x: evlal_duration(x['start'], x['finish']), axis =  1)

    df['OperDayDate'] = df['start'].dt.date

    return df

def get_record_link(record):
    #record_id = base64.b64decode(record.encode()).decode("utf-8")
    record_id = record
    json_query = '{  \
                "recording_id": "' + record_id  + '",  \
                "action": "download"  \
            }'.translate(string.whitespace)

    r = mango_call(json_query, "queries/recording/post")    
    return r 


get_variables()

for x in range_dates(params["date_from"], params["date_to"]):
    print ('Date interval from  ' , str(x[0]) , '  till  ' , str(x[1]))
    callback = request_calls(x[0], x[1])
    resp = request_callback(callback)
    df = df.append(text_to_df(resp))

df=df[(df.duration>30)&((df.disconnect_reason==1110)|(df.disconnect_reason==1120))]

for index, row in df.iterrows():
    folder = '{0}-{1:02d}-{2}'.format(row.OperDayDate.year,row.OperDayDate.month,row.OperDayDate.day)
    if not os.path.exists("./"+folder):
        os.mkdir("./"+folder)

    for rec in row.records:
        if rec:
            r = get_record_link(rec)
            with open("./"+folder+"/"+rec+'.mp3', 'wb') as f:
                f.write(r.content)

df.to_json('records.json')
