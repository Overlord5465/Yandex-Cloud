import json
import ydb
import os
from datetime import datetime, timedelta

def handler(event, context):
    try:
        query_params = event.get('queryStringParameters', {})
        student_id = query_params.get('student_id')
        
        if not student_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'student_id is required'})
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
        
        query = "SELECT * FROM journal WHERE student_id = '" + student_id + "'"
        result = session.transaction().execute(query, commit_tx=True)
        
        grades = []
        for row in result[0].rows:
            raw_date = str(row['date'])
            try:
                if raw_date.isdigit():
                    days = int(raw_date)
                    base_date = datetime(1970, 1, 1)
                    normal_date = (base_date + timedelta(days=days)).strftime('%Y-%m-%d')
                else:
                    normal_date = raw_date
                
                # Корректируем дату - вычитаем 1 день для компенсации часового пояса
                date_obj = datetime.strptime(normal_date, '%Y-%m-%d')
                corrected_date = (date_obj - timedelta(days=1)).strftime('%Y-%m-%d')
                
            except:
                corrected_date = raw_date
            
            grades.append({
                'subject': str(row['subject']),
                'mark': str(row['mark']),
                'date': corrected_date,
                'teacher_id': str(row['teacher_id'])
            })
        
        driver.stop()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'grades': grades
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
