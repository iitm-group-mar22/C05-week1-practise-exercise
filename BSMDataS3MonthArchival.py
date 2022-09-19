import csv
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import datetime
import os
import json
import itertools
import statistics

try:
        # Here we assign our aws clients/resources to use
        iot_client = boto3.client('iot',region_name ='us-east-1')
        s3 = boto3.resource(service_name = 's3')
        dynamodb_resource = boto3.resource('dynamodb',region_name='us-east-1')
        table = dynamodb_resource.Table('bsm_data')

        # Getting current date and previous date
        #current_date = datetime.date.today()
        current_date = datetime.date(2022,10,10)
        first_day_of_month = current_date.replace(day=1)

        if(first_day_of_month==current_date):

            prev_month = first_day_of_month - datetime.timedelta(days=1)
            first_day_of_prev_month = prev_month.replace(day=1)

            prevMonth = first_day_of_month.month
            prevYear = first_day_of_month.year


            # retrive unique device ids from the IOT Core
            response = iot_client.list_things(maxResults=100, thingTypeName='BedSideMonitor')
            devices = response["things"]

            device_ids = []
            for y in devices:
                    device_id = y["thingName"]
                    device_ids.append(device_id)

            for device_id in device_ids:
                    #condition = Key('deviceid').eq(device_id)& Key('timestamp').between('2022-09-1','2022-09-30')
                    #condition = Key('deviceid').eq(device_id)& Key('timestamp').between(str(previous_date),str(current_date))
                    condition = Key('deviceid').eq(device_id)& Key('timestamp').between(str(first_day_of_prev_month), str(first_day_of_month))
                    responsevalues = table.query(KeyConditionExpression=condition)
                    if len(responsevalues['Items']) != 0:
                            items = responsevalues['Items']
                            columns = ['timestamp', 'avg_value']
                            key = "HealthCareDataMonthlyArchive/"+ device_id + "/" + str(prevYear) + "/"
                            filename = device_id + "-" + str(prevYear) + "-" + str(prevMonth) + ".csv"
                            with open(filename,'a') as f:
                                    dict_writer = csv.DictWriter(f, columns)
                                    dict_writer.writeheader()
                                    items_by_day = itertools.groupby(items, key=lambda x: x["timestamp"][:10])
                                    for day, items in items_by_day:
                                        values_per_day = [item["value"] for item in items]
                                        avg = statistics.mean(values_per_day)
                                        l={'timestamp': day, 'avg_value': avg}
                                        dict_writer.writerow(l)
                            s3.meta.client.upload_file(Filename = filename,Bucket="bedsidemonitorgl",Key=key+filename)
        else:
            print("You need to run on the first day of the month")
except ClientError as e:
        print("deer")
