from postgres_service import PostgresService
from constant import WHITE_LIST_DB
def get_list_db(file_path='db_list.txt'):
    list_db = []
    with open(file_path, 'r') as f:
        for line in f.readlines():
            if line == '':
                continue
            list_db.append(line.strip())
    return list_db


list_db = get_list_db()
service = PostgresService()
service.create_connection()
dbs = service.get_all_db()
current_db = []
for db in dbs:
    current_db.append(db[0]) if db[0] not in WHITE_LIST_DB else None
service.close_connection()


add_db = [db for db in list_db if db not in current_db]
remove_db = [db for db in current_db if db not in list_db]
stay_db = [db for db in current_db if db in list_db]

for db in add_db:
    print(f'Add in : {db}')
    service.on_create_db(db)

for db in remove_db:
    service.create_connection()
    service.delete_db(db)
    service.close_connection()
    print(f'Remove in : {db}')

for db in stay_db:
    print(f'Stay in : {db}')
