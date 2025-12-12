import json
import ydb
import os

def handler(event, context):
    try:
        body = json.loads(event['body'])
        login = body['login']
        password = body['password']
        
        driver_config = ydb.DriverConfig(
            endpoint=os.getenv('YDB_ENDPOINT'),
            database=os.getenv('YDB_DATABASE'),
            credentials=ydb.credentials_from_env_variables()
        )
        
        driver = ydb.Driver(driver_config)
        driver.wait(fail_fast=True, timeout=10)
        
        session = driver.table_client.session().create()
        
        query = f"SELECT * FROM students WHERE login = '{login}'"
        result = session.transaction().execute(query, commit_tx=True)
        
        user_type = 'student'
        if not result[0].rows:
            query = f"SELECT * FROM teachers WHERE login = '{login}'"
            result = session.transaction().execute(query, commit_tx=True)
            user_type = 'teacher'
        
        if not result[0].rows:
            driver.stop()
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'User not found'})
            }
        
        user = result[0].rows[0]
        
        if user['password'] != password:
            driver.stop()
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Invalid password'})
            }
        
        user_data = {
            'id': str(user['id']),
            'full_name': str(user['full_name']),
            'type': user_type
        }
        
        if user_type == 'student':
            user_data['class'] = str(user['class'])
        else:
            user_data['subject'] = str(user['subject'])
        
        driver.stop()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'user': user_data,
                'role': user_type
            })
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
