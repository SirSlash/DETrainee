import sys
import os
import pandas as pd
from mysql import connector as mc
from mysql.connector import errorcode as ec
from sqlalchemy import create_engine
import json

from lxml import etree
from lxml.builder import E as buildE, unicode

DB_DETAILS = {
    'DB_HOST': '127.0.0.1',
    'DB_NAME': 'dorm',
    'DB_USER': 'root',
    'DB_PASS': '123',
    'DB_PORT': '3306'
}


def query(query_number):
    case_query = ''
    if query_number == 0:
        """    Список комнат и количество студентов в каждой из них """
        case_query = '''
                SELECT r.name as room, count(s.room) as students_count
                FROM rooms r
                LEFT JOIN students s
                    ON r.id = s.room
                GROUP BY r.name
                '''

    elif query_number == 1:
        """    5 комнат, где самый маленький средний возраст студентов """
        case_query = '''
                SELECT r.name as room,
                    ROUND(AVG(DATE_FORMAT(FROM_DAYS(DATEDIFF(NOW(),s.birthday)), '%Y')), 2) as avg_age
                FROM students s
                LEFT JOIN rooms r
                    ON r.id = s.room
                GROUP BY r.id, r.name
                ORDER BY avg_age ASC
                LIMIT 5
                '''

    elif query_number == 2:
        """    5 комнат с самой большой разницей в возрасте студентов """
        case_query = '''
                SELECT r.name as room, MAX(a.age) - MIN(a.age) as age_diff
                FROM (
                        SELECT s.id, s.room,
                            DATE_FORMAT(FROM_DAYS(DATEDIFF(NOW(),s.birthday)), '%Y') as age
                        FROM students s
                    ) as a
                LEFT JOIN rooms r
                    ON r.id = a.room
                GROUP BY a.room, r.name
                ORDER BY age_diff DESC
                LIMIT 5;
                '''

    elif query_number == 3:
        """    Список комнат где живут разнополые (однополые!) студенты """
        case_query = '''
                SELECT r.name as same_sex_room
                FROM students s
                LEFT JOIN rooms r
                    ON r.id = s.room
                GROUP BY s.room, r.name
                HAVING COUNT(DISTINCT s.sex) = 1
                '''

    return case_query


def query_handler(mysql_obj, query_number):
    cursor = mysql_obj.connection.cursor()

    task_query = query(query_number)

    cursor.execute(task_query)
    rows = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]
    result = []
    for row in rows:
        record = dict(zip(columns, row))
        result.append(record)

    return result


def save_json_file(mysql_obj, task_number):
    result = query_handler(mysql_obj, task_number)
    json_data = json.dumps(result, indent=4)

    file = open(f'query_{task_number + 1}.json', 'w')
    file.write(json_data)
    file.close()
    return


def tag_builder(tag, parent=None, content=None):
    element = buildE(tag)
    if content is not None:
        element.text = unicode(content)
    if parent is not None:
        parent.append(element)
    return element


def fetch_xml(cursor):
    fields = [x for x in cursor[0]]
    doc = tag_builder('data')
    for record in cursor:
        r = tag_builder('row', parent=doc)
        for (k, v) in zip(fields, record.values()):
            tag_builder(k, content=v, parent=r)
    return doc


def save_xml_file(mysql_obj, task_number):
    result = query_handler(mysql_obj, task_number)

    doc = fetch_xml(result)
    string_data = etree.tostring(doc, pretty_print=True)

    file = open(f'query_{task_number + 1}.xml', 'wb')
    file.write(string_data)
    file.close()
    return


class MySQL:

    def __init__(self, db_details):
        self.db_details = db_details

        try:
            self.connection = mc.connect(host=db_details['DB_HOST'],
                                         database=db_details['DB_NAME'],
                                         user=db_details['DB_USER'],
                                         password=db_details['DB_PASS'],
                                         port=db_details['DB_PORT']
                                         )
        except mc.Error as error:
            if error.errno == ec.ER_ACCESS_DENIED_ERROR:
                print('Invalid Credentials')
            else:
                print(error)

        engine_config = f"mysql+mysqlconnector://{self.db_details['DB_USER']}:{self.db_details['DB_PASS']}@{self.db_details['DB_HOST']}/{self.db_details['DB_NAME']}"
        self.engine = create_engine(engine_config)

    def create_table(self):
        cursor = self.connection.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS rooms(id INTEGER(64) PRIMARY KEY, name VARCHAR(255))")
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS students(id INTEGER(64) PRIMARY KEY,
                                                        name VARCHAR(255),
                                                        birthday DATETIME,
                                                        sex VARCHAR(8),
                                                        room INTEGER(64),
                                                        FOREIGN KEY (room)
                                                            REFERENCES rooms(id)
                                                            ON DELETE CASCADE)
                        """)

    def create_index(self):
        cursor = self.connection.cursor()
        index_query = 'CREATE INDEX idx_room ON students (room)'
        cursor.execute(index_query)

    def load_data_to_table(self, t_name, t_data, if_exists):
        t_data.to_sql(name=t_name, con=self.engine, if_exists=if_exists, index=False)


def main():
    # file_names = ['rooms.json', 'students.json']
    # file_type = 'xml'
    file_names = [os.path.basename(sys.argv[1]), os.path.basename(sys.argv[2])]
    file_type = sys.argv[3]

    all_df_data = {}
    for file in file_names:
        table_name = os.path.splitext(file)[0]
        all_df_data[table_name] = pd.read_json(file)

    mysql_obj = MySQL(DB_DETAILS)
    mysql_obj.create_table()
    mysql_obj.create_index()
    for t_name, t_data in all_df_data.items():
        mysql_obj.load_data_to_table(t_name, t_data, 'replace')

    for task_number in range(4):
        if file_type == 'json':
            save_json_file(mysql_obj, task_number)
        elif file_type == 'xml':
            save_xml_file(mysql_obj, task_number)
        else:
            print("Incorrect File Type!")

    mysql_obj.connection.close()


if __name__ == '__main__':
    main()
