import psycopg2
import pandas as pd
from tqdm import tqdm
import numpy as np
from pathos.multiprocessing import ProcessingPool as Pool


### Database #################################################################################################################
def get_cursor_raw():
    host = ""
    database = ""
    user = ""
    port = 0
    connection = psycopg2.connect(host=host,
                                  database=database,
                                  user=user,
                                  port=port)
    cursor = connection.cursor()
    return connection, cursor

def single_insert(insert_req):
    """ Execute a single INSERT request """
    connection, cursor = get_cursor_raw()
    try:
        cursor.execute(insert_req)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
        return 1
    connection.close()
##############################################################################################################################



### Data ################################################################################################################
filename = "/home/daniel/projects/github/IEX_pcap/quotes.csv"
data = pd.read_csv(filename ,  encoding = "utf-8")

# data pre processing #
N = len(data.index)

print("Total number of new entries:",N)

grid = np.linspace(0,N,round(N/5000), dtype=int)
data_indices = []
for k in tqdm(range(len(grid)-1)):
    data_indices.append(data.index[grid[k]:grid[k+1]])
#########################################################################################################################



### Parallel ################################################################################################################
def parallel_storage(data_indices):
    query = """ INSERT into iex_historical_orderbook (timestamp,symbol, side, flag, size, price) values """
    for i in data_indices:
            if i>data_indices[0]:
                query += ","
            query += "({},'{}','{}','{}',{},{})".format(    data['Timestamp'][i],
                                                            data['Symbol'][i],
                                                            data['Side'][i],
                                                            data['EventFlags'][i],
                                                            data['Size'][i],
                                                            data['Price'][i])
    query += ' ON CONFLICT DO NOTHING;'
    single_insert(query)

# run inserts in parallel
p = Pool(nodes=int(6))
with Pool(5) as p:
    _max= len(data_indices)
    r = list(tqdm(p.imap(parallel_storage, data_indices), total=_max))
############################################################################################################################






### Serial ####

#
# for k in tqdm(range(len(grid)-1)):
#
#     query = """ INSERT into iex_historical_orderbook(timestamp,symbol, side, flag, size, price) values """
#
#     data_indices = data.index[grid[k]:grid[k+1]]
#
#     for i in data_indices:
#
#         if i>data_indices[0]:
#
#             query += ","
#
#         query += "({},'{}','{}','{}',{},{})".format(  data['Timestamp'][i],
#                                                         data['Symbol'][i],
#                                                         data['Side'][i],
#                                                         data['EventFlags'][i],
#                                                         data['Size'][i],
#                                                         data['Price'][i])
#     query += ' ON CONFLICT DO NOTHING;'
#
#     single_insert(connection, query)

# connection.close()
