import pytest
from app.config import AppConfig, TestMode


class TestAppConfig:

    def test_change_test_mode(self):
        assert AppConfig.set_test_mode( TestMode.UNIT_TEST )
        assert AppConfig.is_client_test() is False
        assert AppConfig.is_unit_test() is True
        assert AppConfig.is_production() is False

        assert AppConfig.set_test_mode( TestMode.CLIENT_TEST )
        assert AppConfig.is_client_test() is True
        assert AppConfig.is_unit_test() is False
        assert AppConfig.is_production() is False

        AppConfig.set_test_mode(TestMode.PRODUCTION)
        assert AppConfig.get_test_mode() == TestMode.PRODUCTION
        assert AppConfig.is_client_test() is False
        assert AppConfig.is_unit_test() is False
        assert AppConfig.is_production() is True
