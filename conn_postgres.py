import psycopg2

def open_pg_conn(host, port, dbname, user, password):
###Open a postgres connection
    try:
        db_conn = psycopg2.connect(host=host, 
                        port=port,
                        dbname=dbname, 
                        user=user, 
                        password=password)
        
    except psycopg2.Error as e:
        message = "Connection error: " + e 
    return db_conn