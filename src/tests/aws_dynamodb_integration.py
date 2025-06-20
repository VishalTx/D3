from utils.aws_wrapper import AWSDynamoDB

def test_dynamodb_list_tables():
    aws_object = AWSDynamoDB()
    tables = aws_object.dynamodb.list_tables()
    # printing the list of table to DEBUG only
    print( tables )
    assert len(tables) > 0

def test_dynamodb_table_exist_success():
    aws_object = AWSDynamoDB()
    table_name = 'ddm_client_trigger_config'
    response = aws_object.table_exists(table_name)
    assert response == True

def test_dynamodb_table_exist_error():
    aws_object = AWSDynamoDB()
    table_name = 'table_does_not_exists'
    response = aws_object.table_exists(table_name)
    assert response == False

def test_dynamodb_table_info():
    aws_object = AWSDynamoDB()
    table_name = 'ddm_client_trigger_config'
    response = aws_object.table_info(table_name)
    print(response)
    assert table_name == response['Table']['TableName']

'''
Sample Table Response from DynamoDB API
=======================================
Table
 - AttributeDefinitions
 - TableName
 - KeySchema: [{'AttributeName': 'config_id', 'KeyType': 'HASH'}]
 - TableStatus
 - CreationDateTime
 - ProvisionedThroughput
 - TableSizeBytes
 - ItemCount
 - TableArn # arn:aws:dynamodb:us-east-1:590407490654:table/ddm_client_trigger_config
 - TableId  # 1c9b8ebd-f786-48b6-9195-a18f1d5981f0
'''
