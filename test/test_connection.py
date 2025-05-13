import unittest
from unittest.mock import patch, MagicMock
from src.connection import create_con, close_db

class TestDBConnection(unittest.TestCase):
    
    @patch.dict('os.environ', {
        "PGUSER": "test_user",
        "PGPASSWORD": "test_password",
        "PGDATABASE": "test_db"
    })
    @patch('src.connection.Connection')
    def test_create_con(self, mock_connection):
        mock_conn_instance = MagicMock()
        mock_connection.return_value = mock_conn_instance

        con = create_con()

        mock_connection.assert_called_once_with(
            "test_user", 
            password="test_password", 
            database="test_db"
        )
        self.assertEqual(con, mock_conn_instance)

    def test_close_db(self):
        mock_con = MagicMock()
        close_db(mock_con)
        mock_con.close.assert_called_once()
