import json
import ydb
import os
from datetime import datetime

def handler(event, context):
    try:
        body = json.loads(event['body'])
        
        student_id = body.get('student_id')
        subject = body.get('subject')
        mark = body.get('mark')
        teacher_id = body.get('teacher_id')
        date = body.get('date')
        
        if not all([student_id, subject, teacher_id, date]):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required fields'})
            }
        
        ydb_endpoint = "grpcs://ydb.serverless.yandexcloud.net:2135"
        ydb_database = "/ru-central1/b1g55gr5950cipn060hu/etna238vjpc5021bak66"
        
        driver_config = ydb.DriverConfig(
            endpoint=ydb_endpoint,
            database=ydb_database,
            credentials=ydb.credentials_from_env_variables()
        )
        
        driver = ydb.Driver(driver_config)
        driver.wait(fail_fast=True, timeout=10)
        
        session = driver.table_client.session().create()
        
        grade_id = student_id + "_" + date + "_" + subject
        
        if mark:
            query = "UPSERT INTO journal (id, student_id, subject, date, mark, teacher_id) VALUES ('" + grade_id + "', '" + student_id + "', '" + subject + "', Date('" + date + "'), '" + mark + "', '" + teacher_id + "')"
        else:
            query = "DELETE FROM journal WHERE id = '" + grade_id + "'"
        
        session.transaction().execute(query, commit_tx=True)
        
        driver.stop()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': 'Grade updated successfully'
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
