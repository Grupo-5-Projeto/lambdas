import json
import boto3
from decimal import Decimal
from datetime import datetime
from zoneinfo import ZoneInfo 
import os

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ.get("DYNAMO_TABLE_NAME"))

def lambda_handler(event, context):
    print(event, context)
    print("Received event:", json.dumps(event))  # Log do evento completo

    try:
        if isinstance(event, str):
            print("Parsing body string to dict")
            data = json.loads(event)
        elif isinstance(event, dict):
            print("Body is already a dict")
            data = event
        else:
            print("Unsupported body type:", type(event))
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Invalid body type"})
            }

        fk_sensor = data.get("fk_sensor")
        data_hora = data.get("data_hora")
        valor = data.get("valor")
        fk_upa = data.get("fk_upa")
        biometria = data.get("biometria")
   
        print(f"Parsed data - fk_sensor: {fk_sensor}, data_hora: {data_hora}, valor: {valor}, fk_upa: {fk_upa}")

        if fk_sensor is None or data_hora is None or valor is None or fk_upa is None:
            print("Missing required fields")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required fields"})
            }

        fk_unid_medida = data.get("fk_unid_medida")
        fk_paciente = data.get("fk_paciente")

        data = datetime.strptime(data_hora, "%Y-%m-%dT%H:%M:%S")
        data = data.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
        timestamp = data.timestamp()
      
        item = {
            "fk_sensor": int(fk_sensor),
            "data_hora": data_hora,
            "valor": Decimal(str(valor)),
            "fk_upa": int(fk_upa),
            "timestamp": Decimal(str(timestamp))
        }

        if fk_unid_medida is not None:
            item["fk_unid_medida"] = int(fk_unid_medida)
        if fk_paciente is not None:
            item["fk_paciente"] = int(fk_paciente)
        if biometria is not None:
            item["biometria"] = biometria

        print("Putting item into DynamoDB:", item)
        table.put_item(Item=item)
        print("Item successfully stored")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Event stored"})
        }

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error"})
        }