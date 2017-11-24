from psycopg2 import sql
import psycopg2.extras
import sys
import csv
from config import *

db_connection = None
db_cursor = None

def get_connection_and_cursor():
  global db_connection, db_cursor
  if not db_connection:
      try:
          db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
          print("Success connecting to database")
      except:
          print("Unable to connect to the database. Check server and credentials.")
          sys.exit(1)

  if not db_cursor:
      db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  return db_connection, db_cursor

def setup_database():
  conn, cur = get_connection_and_cursor()

  cur.execute("DROP TABLE IF EXISTS Sites")
  cur.execute("DROP TABLE IF EXISTS States")

  cur.execute("CREATE TABLE IF NOT EXISTS \
                 Sites(ID SERIAL PRIMARY KEY, \
                       Name VARCHAR(128) UNIQUE, \
                       Type VARCHAR(128), \
                       State_ID INT, \
                       Location VARCHAR(255), \
                       Description TEXT)")

  cur.execute("CREATE TABLE IF NOT EXISTS \
                 States(ID SERIAL PRIMARY KEY, \
                        Name VARCHAR(40) UNIQUE)")

  conn.commit()
  print('Setup database complete')

# The insert function is adapted from twitter_database.py
def insert(conn, cur, table, data_dict, do_return=False):
    column_names = data_dict.keys()
    if do_return:
        query = sql.SQL('INSERT INTO {0}({1}) VALUES({2}) ON CONFLICT DO NOTHING RETURNING id').format(
            sql.SQL(table),
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(map(sql.Placeholder, column_names))
        )
    else:
        query = sql.SQL('INSERT INTO {0}({1}) VALUES({2}) ON CONFLICT DO NOTHING').format(
            sql.SQL(table),
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(map(sql.Placeholder, column_names))
        )
    query_string = query.as_string(conn)
    cur.execute(query_string, data_dict)
    if do_return:
        return cur.fetchone()['id']

setup_database()
states = ['Arkansas', 'Michigan', 'California']

for state in states:
  id = insert(db_connection, db_cursor, 'States', {'name': state}, True)
  filename = state.lower() + '.csv'
  with open(filename, 'r', newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
          insert(db_connection, db_cursor, 'Sites', {'name': row['NAME'],
                                                     'type': row['TYPE'],
                                                     'state_id': id,
                                                     'location': row['LOCATION'],
                                                     'description': row['DESCRIPTION']}
                )
db_connection.commit()

db_cursor.execute("""SELECT Location
                       FROM Sites""")
all_locations = [result['location'] for result in db_cursor.fetchall()]
# print(all_locations)

db_cursor.execute("""SELECT Name
                       FROM Sites
                         WHERE Sites.description like '%beautiful%'""")
beautiful_sites = [result['name'] for result in db_cursor.fetchall()]
# print(beautiful_sites)

db_cursor.execute("""SELECT COUNT(id)
                       FROM Sites
                         WHERE Type='National Lakeshore'""")
natl_lakeshores = db_cursor.fetchone()['count']
# print(natl_lakeshores)

db_cursor.execute("""SELECT Sites.name
                       FROM Sites INNER JOIN States ON (Sites.state_id = States.id)
                         WHERE States.name='Michigan'""")
michigan_names = [result['name'] for result in db_cursor.fetchall()]
# print(michigan_names)

db_cursor.execute("""SELECT COUNT(Sites.id)
                       FROM Sites INNER JOIN States ON (Sites.state_id = States.id)
                         WHERE States.name='Arkansas'""")
natl_lakeshores = db_cursor.fetchone()['count']
# print(natl_lakeshores)
