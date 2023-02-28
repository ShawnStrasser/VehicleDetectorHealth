# This code is optional, it is used to extract filtered data from SQL Server
# Data is saved to parquet file data.parquet

# Run in command line like this:
#   python get_sql_data.py --server server_name --database database_name --devices device_id_1 device_id_2 ... device_id_n
# For example: 
#   python get_sql_data.py --server myserver --database mydatabase --devices 240 241 242 243 244 245 246

import pandas as pd
from sqlalchemy import create_engine, text
import argparse

def get_data(server, database, query):
    # set up the database connection
    connection_string = f"mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=SQL+Server"
    engine = create_engine(connection_string)
    # execute a SQL query and retrieve the data into a pandas DataFrame
    df = pd.DataFrame(engine.connect().execute(text(query)))
    # close the connection
    engine.dispose()
    return df

def sql(device_id):
    '''
    Returns previous week of data for list of DeviceId'server
    Has the following filters:
    Tuesday, Wednesday, and Thursday, from 6:00 AM to 9:00 and 4:00 PM to 7:00 PM
    EventId IN(1, 8, 81, 82)
    '''
    device_id = str(device_id)[1:-1] #extract list into text for query
    query = f"""
        SELECT * 
        FROM ASCEvents 
        WHERE EventId IN (1, 8, 81, 82) 
        AND DeviceId IN({device_id})
        --Previous Tuesday (midnight)
        AND TimeStamp > DATEADD(WEEK, -1, DATEADD(DAY, -(DATEPART(WEEKDAY, GETDATE())) % 7 + 3, CONVERT(DATE, GETDATE())))
        --Previous Friday (midnight)
        AND TimeStamp < DATEADD(WEEK, -1, DATEADD(DAY, -(DATEPART(WEEKDAY, GETDATE())) % 7 + 6, CONVERT(DATE, GETDATE()))) 
        AND (
                (DATEPART(HOUR, TimeStamp) >= 6 AND DATEPART(HOUR, TimeStamp) < 9) 
                OR 
                (DATEPART(HOUR, TimeStamp) >= 16 AND DATEPART(HOUR, TimeStamp) < 19)
        )
    """
    return query

if __name__ == '__main__':
    # define command line arguments
    parser = argparse.ArgumentParser(description='Query data from a SQL Server database')
    parser.add_argument('--server', type=str, help='SQL Server to connect to', required=True)
    parser.add_argument('--database', type=str, help='Database to query', required=True)
    parser.add_argument('--devices', type=int, nargs='+', help='List of device IDs to query', required=True)
    args = parser.parse_args()
    print('Arguments given: \n', args, '\nRunning query now')

    # generate SQL query string from devices argument
    query = sql(args.devices)

    # query database and write result to a parquet file
    get_data(args.server, args.database, query).\
        set_index('TimeStamp').\
        astype({'DeviceId':'uint16', 'EventId':'uint8', 'Parameter':'uint8'}).\
        to_parquet('data.parquet')
    print('Done')