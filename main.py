
# Importing custom functions

from helpers import get_client, get_postgres_engine
from extract import fetch_data
from load import load_csv_to_postgres
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
query = '''
        select pickup_date, vendor_id, passenger_count,trip_distance, payment_type, fare_amount, tip_amount 
        from tripdata 
        where year(pickup_date) = 2015 and month(pickup_date) = 1 and dayofmonth(pickup_date) = 3
        '''

client = get_client()

engine = get_postgres_engine()

def main():
    '''
    Main function to run all the data pipeline modules/logic:
    1. It extracts the data
    2. It loads the data

    Parameters: None

    Return: None
    '''

    # Extract the data 

    fetch_data (client=client, query=query)


    # Load the data 

    load_csv_to_postgres("tripdata.csv", "tripdata", engine, "STG")

    ## execute stored procedure

    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute(text ('call "STG".agg_tripdata()'))
    session.commit()

    print ("Stored procedure executed")

    print ("Pipeline executed successfully")


# Calling our Main Function

if __name__ == "__main__" :
    main() 
    

# professionally you shouldn't just call your functions like this when working with modular codes. 
# to avoid mistakenly importing or executing some functions when not needed.
# so best practice is, when you are calling a function, it is best to wrap it around a 
# clause that tells python that - "only run this function when i execute this script by myself" i.e, 
# only execute the function when i run the script myself
# this clause is called a ""




