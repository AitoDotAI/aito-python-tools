from aito.sdk.aito_client import AitoClient

def aito_get_env(file):
    # Parse URL and KEY from ENV file without external libraries.
    with open(file, 'r') as f:
        content = f.read()
    env_list = [row for row in content.split('\n')]
    keys = [keyval.split(' = ')[0] for keyval in env_list]
    values = [keyval.split(' = ')[1].replace('\'','') for keyval in env_list]
    env_dictionary = dict(zip(keys,values))
    return env_dictionary['AITO_INSTANCE_URL'], env_dictionary['AITO_API_KEY']

def aito_client(url, key):
    # Initiate Aito Client.
    return AitoClient(instance_url=url, api_key=key)

def aito_count_entries(client, table, limit=1):
    # Get the total number of entries in a table.
    response = client.query_entries(table_name=table, limit=limit)
    count = response['total']
    return count

def aito_upload(client, table, inputs):
    # Upload entries to a table.
    # Transform inputs to dictionary and then to list.
    # Inputs must contain a valid value for all columns in the schema.
    inputs = eval(str(inputs))
    if type(inputs) != list: inputs = [inputs]
    client.upload_entries(table_name=table, entries=inputs)

def aito_predict(client, table, inputs, target, limit=1):
    # Make a prediction.
    # Transform inputs to dictionary and remove target column if exists.
    inputs = eval(str(inputs))
    inputs.pop(target, None)
    query = {'from':table,'where':inputs,'predict':target,'limit':limit}
    response = client.request(
                      method='POST',
                      endpoint='/api/v1/_predict',
                      query=query)
    prediction = response['hits'][0]['feature']
    probability = response['hits'][0]['$p']
    return prediction, probability