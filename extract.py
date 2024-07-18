import pandas as pd

# functions to get data

def fetch_data(client, query):

    '''
    Fetches query results from a clickhouse database and writes to a csv file

    Parameters:

    - client (clickhouse_connect.Client)
    - query (A SQL select query)

    Returns: None
    '''

    #  Execute the query

    output = client.query(query)
    rows = output.result_rows
    cols = output.column_names

    ## close the client connection

    client.close()

    ## Write to pandas df and csv file

    df = pd.DataFrame(rows, columns=cols)
    df.to_csv("tripdata.csv", index=False)

    print(f"{len(df)} rows sucessfully extracted")