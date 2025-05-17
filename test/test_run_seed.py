import unittest
from unittest.mock import patch, MagicMock

class TestSeedScript(unittest.TestCase):

    @patch("src.connection.create_con")
    @patch("src.connection.close_db")
    @patch("src.seed.seed")
    @patch("src.data.parks.parks")
    @patch("src.data.rides.rides")
    @patch("src.data.other_fac.other_fac")
    @patch("src.data.shops.shops")
    @patch("src.data.stalls.stalls")
    @patch("src.data.foods.foods")
    @patch("src.data.items.items")
    def test_seed_script_runs_successfully(
        self,
        mock_create_con,
        mock_close_db,
        mock_seed,
        mock_parks,
        mock_rides,
        mock_other_fac,
        mock_shops,
        mock_stalls,
        mock_foods,
        mock_items
    ):
        mock_db = MagicMock()
        mock_create_con.return_value = mock_db

        db = None
        try:
            db = mock_create_con()
            mock_seed(
                db,
                parks=mock_parks,
                rides=mock_rides,
                shops=mock_shops,
                stalls=mock_stalls,
                other_fac=mock_other_fac,
                foods=mock_foods,
                items=mock_items
            )
        except Exception as e:
            self.fail(f"Unexpected error: {e}")
        finally:
            if db:
                mock_close_db(db)

        mock_create_con.assert_called_once()
        mock_seed.assert_called_once_with(
            mock_db,
            parks=mock_parks,
            rides=mock_rides,
            shops=mock_shops,
            stalls=mock_stalls,
            other_fac=mock_other_fac,
            foods=mock_foods,
            items=mock_items
        )
        mock_close_db.assert_called_once_with(mock_db)

    @patch("src.seed.seed", side_effect=Exception("Seed failure"))
    @patch("src.connection.close_db")
    @patch("src.connection.create_con")
    def test_seed_script_handles_exception(self, mock_create_con, mock_close_db, mock_seed):
        mock_db = MagicMock()
        mock_create_con.return_value = mock_db

        db = None
        try:
            db = mock_create_con()
            mock_seed(
                db,
                parks=[],
                rides=[],
                shops=[],
                stalls=[],
                other_fac=[],
                foods=[],
                items=[]
            )
        except Exception as e:
            self.assertEqual(str(e), "Seed failure")
        finally:
            if db:
                mock_close_db(db)

        mock_create_con.assert_called_once()
        mock_seed.assert_called_once()
        mock_close_db.assert_called_once_with(mock_db)