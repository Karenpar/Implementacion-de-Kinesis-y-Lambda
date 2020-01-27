import base64
import json
import boto3
import decimal

sns = boto3.client("sns")
dynamo = boto3.resource('dynamodb')

def lambda_handler(event, context):
    
    item = None
    table = dynamo.Table('compra')
    
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]

    with table.batch_writer() as batch_writer:
        for item in deserialized_data:
			
	    id_compra = item['id_compra']
            fecha_reg = item['fecha_reg']
            producto = item['producto']
            batch_writer.put_item(                        
                Item = {
                            'id_compra': id_compra,
                            'fecha_reg': fecha_reg,
                            'producto': producto
                        }
            )
            
            if producto == 'Raspberry Pi 4B':
                sns.publish(
                    TopicArn = 'arn:aws:sns:us-east-1:XXXXXXXXXX:topicPurchase',    
                    Subject = 'Bienvenido al mundo del IoT',
                    Message = 'La última versión de tu Raspberry está en camino. Gracias por confiar en nosotros.'
                )
