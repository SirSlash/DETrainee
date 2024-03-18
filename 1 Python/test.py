import unittest
from script import MySQL


class MyTestCase(unittest.TestCase):
    def setUp(self):
        db_details = {
            'DB_HOST': '127.0.0.1',
            'DB_NAME': 'dorm',
            'DB_USER': 'root',
            'DB_PASS': '123',
            'DB_PORT': '3306'
        }
        self.mysql_obj = MySQL(db_details)

    def tearDown(self):
        if self.mysql_obj.connection is not None and self.mysql_obj.connection.is_connected():
            self.mysql_obj.connection.close()

    def test_connection(self):
        self.assertTrue(self.mysql_obj.connection.is_connected())

    def test_room_table(self):
        query_count = 'SELECT count(*) from rooms'
        cursor = self.mysql_obj.connection.cursor()
        cursor.execute(query_count)
        count = cursor.fetchone()
        self.assertEqual(count[0], 1000)

    def test_students_table(self):
        query_count = 'SELECT count(*) from students'
        cursor = self.mysql_obj.connection.cursor()
        cursor.execute(query_count)
        count = cursor.fetchone()
        self.assertEqual(count[0], 10000)


if __name__ == '__main__':
    unittest.main()
