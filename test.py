#!/usr/bin/env python
import pika
import sys
import time
import threading
import json
import pymongo
import sys
import os

exit_event = threading.Event()

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["CovidDB"]
myCol = mydb["patients"]
myCol.drop()

current_zip_code_map = dict()
previous_zip_code_map = dict()
zip_map = dict()
alertList = []


# Set the connection parameters to connect to rabbit-server1 on port 5672
# on the / virtual host using the username "guest" and password "guest"

username = 'student'
password = 'student01'
hostname = '128.163.202.50'
virtualhost = '4'

credentials = pika.PlainCredentials(username, password)
parameters = pika.ConnectionParameters(hostname,
                                           5672,
                                           virtualhost,
                                           credentials)





print(' [*] Waiting for logs. To exit press CTRL+C')


def printCurrentZips():
    global previous_zip_code_map
    while True:
        time.sleep(15)
        current_zip_code_map = zip_map.copy()
        zip_map.clear()
        print(current_zip_code_map)
        for key in current_zip_code_map:
            if key in previous_zip_code_map:
                if current_zip_code_map[key] > 2 * previous_zip_code_map:
                    alertList.append(key)
        previous_zip_code_map = current_zip_code_map.copy()
        current_zip_code_map.clear()
        print(previous_zip_code_map)

def findClosestHospital(testedPositive, zip_code, mrn): 
    hospitalColection = mydb["hospitals"]
    hospitals = hospitalColection.distinct("ZIP")
    zipCollection = mydb["kyzipdistance"]
    closestHospital = zipCollection.find({"zip_from": zip_code, 
        "zip_to": {"$in": hospitals}}).sort("distance", 1).limit(1)
    for row in closestHospital:
        hospital = list(hospitalColection.find({"ZIP": row["zip_to"]}))
        for er in hospital:
            beds_taken = myCol.find({ "hospital_id": er["ID"] }).count()
            if beds_taken < er["BEDS"]:
                myQuery = { "mrn": mrn }
                newvalues = { "$set": { "hospital_id": er["ID"], "tested_positive": testedPositive}}
                myCol.update_one(myQuery, newvalues)
                return

def findClosestTraumaCenter(testedPositive, zip_code, mrn):
    hospitalColection = mydb["hospitals"]
    hospitals = hospitalColection.distinct("ZIP", {"TRAUMA": {"$ne": "NOT AVAILABLE"}})
    zipCollection = mydb["kyzipdistance"]
    closestHospital = zipCollection.find({"zip_from": zip_code, 
        "zip_to": {"$in": hospitals}}).sort("distance", 1).limit(1)
    for row in closestHospital:
        hospital = list(hospitalColection.find({"ZIP": row["zip_to"]}))
        for er in hospital:
            beds_taken = myCol.find({ "hospital_id": er["ID"] }).count()
            if beds_taken < er["BEDS"]:
                myQuery = { "mrn": mrn }
                newvalues = { "$set": { "hospital_id": er["ID"], "tested_positive": testedPositive}}
                myCol.update_one(myQuery, newvalues)
                return

def insertPositive(zip_code):
    if zip_code in zip_map:
        zip_map[zip_code] += 1
    else:
        zip_map[zip_code] = 1


def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))
    bodystr = body.decode('utf-8')
    data = json.loads(bodystr)

    for payload in data:
        mrn = payload['mrn']
        patient_status = payload['patient_status_code']
        zip_code = payload['zip_code']
        
        zip_node_id = myCol.insert_one(payload)
        patient_status = payload['patient_status_code']
        
        if patient_status == '0':
            myQuery = { "mrn": mrn }
            newvalues = { "$set": { "hospital_id": 0}}
            myCol.update_one(myQuery, newvalues)
        elif patient_status == '1':
            myQuery = { "mrn": mrn }
            newvalues = { "$set": { "hospital_id": 0, "tested_positive": False}}
            myCol.update_one(myQuery, newvalues)
        elif patient_status == '2':
            myQuery = { "mrn": mrn }
            newvalues = { "$set": { "hospital_id": 0, "tested_positive": True}}
            insertPositive(zip_code)
            myCol.update_one(myQuery, newvalues)
        elif patient_status == '3':
            findClosestHospital(False, int(zip_code), mrn)
        elif patient_status == '4':
            myQuery = { "mrn": mrn }
            newvalues = { "$set": { "hospital_id": 0, "tested_positive": False}}
            myCol.update_one(myQuery, newvalues)
        elif patient_status == '5':
            findClosestHospital(True, int(zip_code), mrn)
            insertPositive(zip_code)
        elif patient_status == '6':
            findClosestTraumaCenter(True, int(zip_code), mrn)
            insertPositive(zip_code)
                
        print("\tInsert MRN: " + mrn + "-> zip_code: " + str(zip_code))


def main():
    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()

    exchange_name = 'patient_data'
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    binding_keys = "#"
    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=binding_key)
        thread = threading.Thread(target=printCurrentZips)
        thread.daemon = True
        thread.start()
        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try: 
            sys.exit(0)
        except SystemExit:
            os._exit(0)

