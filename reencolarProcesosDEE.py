import base64
import json
import os
import shutil
from datetime import datetime

import pyotp
import requests
import boto3
import pyodbc as db
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv() 

# Credential DIAN
URL_CONSULT_DOCUMENT_STATE = os.getenv("URL_CONSULT_DOCUMENT_STATE")
URL_USER_NAME_AUTH = os.getenv("URL_USER_NAME_AUTH")
URL_USER_PASSWORD_AUTH = os.getenv("URL_USER_PASSWORD_AUTH")
URL_SEED = os.getenv("URL_SEED")

AWS_REGION = os.getenv("AWS_REGION")
# Credential AWS SQS
AWS_SESSION_SQS = boto3.Session(
    aws_access_key_id=os.getenv("AWS_CADENA_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_CADENA_SECRET_KEY"),
    region_name=AWS_REGION
).client('sqs')

# Credential AWS S3
AWS_SESSION_S3 = boto3.Session(
    aws_access_key_id=os.getenv("AWS_EFACTURA_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_EFACTURA_SECRET_KEY"),
    region_name=AWS_REGION
).client('s3')

# Creedential SQL Server
SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USERNAME = os.getenv("DB_USERNAME")
PASSWORD = os.getenv("DB_PASSWORD")
DB_CONNECTION_STRING = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

# Read lines from a csv file
def read_csv(input_file:str, has_header:bool=True):
    try:
        with open(f'input_csv/{input_file}', 'r') as f:
            lines = f.readlines()

        if(has_header):
            lines = lines[1:]

        print("Number of lines read: ", len(lines))
        
        return lines
    except Exception as e:
        print(f"read_csv__Error al leer el archivo csv: {e}")
        return None

# Search the state of the register in Efactura
def search_dian_state(lines: list, actual_file: str):
    response_dian = []
    response_error_dian = []
    message_id_list = []
    date_directory = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(f"output_data/{date_directory}"):
        os.makedirs(f"output_data/{date_directory}")

    directory = actual_file.split('.')[0]
    os.makedirs(f"output_data/{date_directory}/{directory}") if not os.path.exists(
        f"output_data/{date_directory}/{directory}") else None
    try:
        
        register_list = []
        for line in tqdm(lines, desc="Building register list"):
            id_unico = line.split(';')[2].strip().replace('"', '')
            emission_nit = line.split(';')[0].strip().split('_')[0].replace('"', '')
            document_type = line.split(';')[1].strip().replace('"', '')

            register_list.append({
                'id_unico': id_unico,
                'emission_nit': emission_nit,
                'document_type': document_type
            })
        
        #print(register_list)
        response_dian, response_error_dian = get_information_from_dian(register_list)
        
        if(len(response_dian) > 0):
            register_steps = search_database_state(response_dian)
            sqs_messages = generate_queue_message(register_steps)
            message_id_list = enqueue_sqs_messages(sqs_messages)

        with open(f"output_data/{date_directory}/{directory}/response_dian.json", 'a+') as f:
            f.write(json.dumps(response_dian, indent=4))

        if len(response_error_dian) > 0:
            with open(f"output_data/{date_directory}/{directory}/response_error_dian.json", 'a+') as f:
                f.write(json.dumps(response_error_dian, indent=4))

        with open(f"output_data/{date_directory}/{directory}/message_ids.json", 'a+') as f:
            f.write(json.dumps(message_id_list, indent=4))

        shutil.move(f"input_csv/{actual_file}", f"processed_csv/{actual_file}")

        print("Processing completed")

    except Exception as e:

        shutil.move(f"input_csv/{actual_file}", f"error_csv/{actual_file}")

        if len(response_dian) > 0:
            with open(f"output_data/{date_directory}/{directory}/response_dian.json", 'a+') as f:
                f.write(json.dumps(response_dian, indent=4))

        if len(response_error_dian) > 0:
            with open(f"output_data/{date_directory}/{directory}/response_error_dian.json", 'a+') as f:
                f.write(json.dumps(response_error_dian, indent=4))

        if len(message_id_list) > 0:
            with open(f"output_data/{date_directory}/{directory}/message_ids.json", 'a+') as f:
                f.write(json.dumps(message_id_list, indent=4))

        print(f"update_adurl__Error en el procesamiento {e}")

# Search the register state in the database
def search_database_state(response_dian_states: list):
    try:
        register_steps = []
        conn = db.connect(DB_CONNECTION_STRING)
        for dian_response in tqdm(response_dian_states, total=len(response_dian_states), desc="Searching database state"):
            id_transmision = dian_response.get('respuestaDIAN').get('idTrasmision')
            track_id = dian_response.get('respuestaDIAN').get('trackId')
            process_type = dian_response.get('process_type')
            register_state = get_register_state(conn, id_transmision, track_id)
            if(register_state is None):
                #print(f"IdTransmision: {id_transmision} - TrackId: {track_id} - StepId: 0")
                register_steps.append({
                    'id_transmision': id_transmision,
                    'track_id': track_id,
                    'step_id': 0,
                    'prod_id': 0,
                    'subprod_id': 0,
                    'dian_response': dian_response,
                    'process_type': process_type
                })
                continue
            #print(f"IdTransmision: {id_transmision} - TrackId: {track_id} - StepId: {register_state.step_id}")
            register_steps.append({
                'id_transmision': id_transmision,
                'track_id': track_id,
                'step_id': register_state.step_id,
                'prod_id': register_state.prod_id,
                'subprod_id': register_state.subprod_id,
                'dian_response': dian_response,
                'process_type': process_type
            })
                
        conn.close()      
        return register_steps
    except Exception as e:
        print(f"Error has been thrown searching registers states: {e}")

# Search the last step of the register in the database
def get_register_state(conn: db.Connection, id_transmision: str, track_id: str):
    try:
        cursor = conn.cursor()
        sql_query = f"SELECT MAX(StepId) AS step_id, p.ProductId AS prod_id, sp.SubProductId AS subprod_id FROM [DATACOM].[dbo].[SeguimientoOndemand] so INNER JOIN [DATACOM].[dbo].[Cycles] cy ON cy.IdTransmision = so.IdTransmision INNER JOIN [ADMINCOM].[dbo].[Customers] cu ON cu.Nit = cy.CustomerName INNER JOIN [ADMINCOM].[dbo].[Product] p ON p.Name = cy.ProductName AND cu.Nit = p.CustomerNit INNER JOIN [ADMINCOM].[dbo].[SubProduct] sp ON p.ProductId = sp.ProductId WHERE so.[IdTransmision] = '{id_transmision}' AND [IdUnico] = '{track_id}' GROUP BY p.ProductId, sp.SubProductId"
        cursor.execute(sql_query)
        db_result = cursor.fetchone()
        cursor.close()
        return db_result
    except Exception as e:
        print(f"Error has been thrown during database query: {e}")
        return None
 
# Update register state in dataChannel table   
def update_register_channel_state(conn: db.Connection, id_transmision: str, track_id: str):
    try:
        cursor = conn.cursor()
        sql_query = f"UPDATE DATACOM.dbo.DataChannel_{id_transmision} SET Estado = 0 WHERE Canal_datos = 'PE' AND IdentificadorUnico IN ( SELECT IdUnico FROM DATACOM.dbo.Data_{id_transmision}_Spool WHERE UCID = '{track_id}')"
        cursor.execute(sql_query)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error has been thrown during database query: {e}")
        return None

# Generate messages to send to the SQS queue
def generate_queue_message(register_steps: list):
    sqs_messages = []
    try:
        conn = db.connect(DB_CONNECTION_STRING)
        for step_found in tqdm(register_steps, total=len(register_steps), desc="Generating messages to SQS"):
            process_type = step_found.get('process_type')
            dian_response = step_found.get('dian_response')
            
            if(process_type == "batch"):
                
                id_transmision = dian_response.get('respuestaDIAN').get('idTrasmision')
                track_id = dian_response.get('respuestaDIAN').get('trackId')
                update_register_channel_state(conn, id_transmision, track_id)
                sqs_messages.append({
                    "queue_url": os.getenv("AWS_SQS_URL_DIAN_RESPONSE"),
                    "message": dian_response.get('respuestaDIAN')
                })
                continue
            
            step_id = int(step_found.get('step_id'))
            match step_id:
                case value if value is None or value < 2:
                    sqs_messages.append({
                        "queue_url": os.getenv("AWS_SQS_URL_PARSER"),
                        "message": dian_response
                    })
                case value if value < 3:
                    sqs_messages.append({
                        "queue_url": os.getenv("AWS_SQS_URL_DIAN_RESPONSE"),
                        "message": dian_response.get('respuestaDIAN')
                    })
                case value if value < 4:
                    sqs_messages.append({
                        "queue_url": os.getenv("AWS_SQS_URL_CHANNELS"),
                        "message":{
                            "RegisterId":step_found.get('track_id'),
                            "IsReprocess":False,
                            "ProcessType":"Ondemand",
                            "ProcessId":step_found.get('id_transmision'),
                            "Channel":"PE",
                            "Part":"1",
                            "IsResubmit":False,
                            "IsFullReset":False,
                            "IsApproved":False,
                            "IsDEProcess":True,
                            "IsAnEventuality":False
                        }
                        
                    })
                case value if value < 5:
                    sqs_messages.append({
                        "queue_url": os.getenv("AWS_SQS_URL_COMPOSER"),
                        "message": {
                            "RetryNumber":0,
                            "ProcessType":"Ondemand",
                            "ProcessId":step_found.get('id_transmision'),
                            "Channel":"PE",
                            "Part":0,
                            "Segment":step_found.get('subprod_id'),
                            "IsResubmit":False,
                            "IsDEProcess":True,
                            "RegisterId":step_found.get('track_id'),
                            "IsAnEventuality":False
                        }
                    })
                case _:
                    sqs_messages.append({
                        "queue_url": os.getenv("AWS_SQS_URL_REPORT"),
                        "message": {
                            "IdTransmision":step_found.get('id_transmision'),
                            "TrackId":step_found.get('track_id'),
                            "Segment":step_found.get('subprod_id'),
                            "IsResubmit":False,
                            "IgnoreDistribution":False,
                            "ProcessType":"Ondemand",
                            "ProcessId":step_found.get('id_transmision'),
                            "RegisterId":step_found.get('track_id'),
                            "Channel":"PE",
                            "IsDEProcess":True,
                            "IsAnEventuality":False
                        }
                    })
        
        conn.close()
        return sqs_messages
        
    except Exception as e:
        print(f"Error has been thrown during queue message generation: {e}")
        return None
        
# Send messages to the SQS queue
def enqueue_sqs_messages(sqs_messages: list):
    message_id_list = []
    try:
        for message in tqdm(sqs_messages, total=len(sqs_messages), desc="Sending messages to SQS"):
            response_queue = AWS_SESSION_SQS.send_message(
                QueueUrl=message.get('queue_url'),
                MessageBody=json.dumps(message.get('message'))
            )
            message_id_list.append(response_queue)

        return message_id_list

    except Exception as e:
        if message_id_list:
            with open('output_data/message_ids.json', 'a+') as file:
                file.write(json.dumps(message_id_list, indent=4))
        print(f"queue_sqs_message__Error al enviar mensajes a SQS: {e}")
    
# Generate otp token and Efactura creedentials
def generate_credentials():
    try:
        totp = pyotp.TOTP(URL_SEED, interval=30, digits=6, digest='SHA1')
        credentials = f"{URL_USER_NAME_AUTH}:{URL_USER_PASSWORD_AUTH}:{totp.now()}"
        credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        return credentials
    except Exception as e:
        print(f"generate_credentials__Error al generar el token de acceso: {e}")
        return None

# Search DIAN state in Efactura database
def get_information_from_dian(information_list: list):
    try:
        response_dian = []
        response_error_dian = []
        for information in tqdm(information_list, total=len(information_list), desc="Getting information from DIAN"):
            credentials = generate_credentials()
            if credentials:
                url = f"{URL_CONSULT_DOCUMENT_STATE}?nit_emisor={information.get('emission_nit')}&id_documento={information.get('id_unico')}&codigo_tipo_documento={information.get('document_type')}"
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Basic {credentials}'
                }
                response = requests.get(url, headers=headers)
                response = response.json()
                if response.get('statusCode') in ['200', '409']:
                    response['customerTyn'] = information.get('emission_nit', "")
                    response['product'] = response.get('idTrasmision', "").split('_')[1]
                    response['isReprocess'] = True
                    process_type = get_process_type(response.get('process_type', ""))
                    response_dian.append({
                        "respuestaDIAN": response,
                        "process_type": process_type
                    })
                else:
                    response_error_dian.append({
                        "respuestaDIAN": response,
                        "nit_emisor": information.get('emission_nit'),
                        "id_documento": information.get('id_unico'),
                        "document_type": information.get('document_type')
                    })

        return response_dian, response_error_dian
    except Exception as e:
        print(f"get_information_from_dian__Error al obtener la información de DIAN: {e}")
        raise Exception(f"get_information_from_dian__Error al obtener la información de DIAN: {e}")

def get_process_type(type_found : str):
    type_found = type_found.lower()
    if( type_found != "ondemand"):
        return "batch"
    return type_found

if __name__ == '__main__':
   
    file_name = "prueba.csv"
    has_header = False
    lines_read = read_csv(file_name, has_header)
    if(lines_read is None):
        print("Error during file reading. Cannot continue")
        exit()
    search_dian_state(lines_read, file_name)