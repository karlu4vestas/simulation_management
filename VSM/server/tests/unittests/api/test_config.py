import pytest
from server.app.app_config import AppConfig, AppTestMode


class TestAppConfig:

    def test_change_test_mode(self):
        assert AppConfig.set_test_mode(AppTestMode.UNIT_TEST)
        assert AppConfig.is_client_test() is False
        assert AppConfig.is_unit_test() is True
        assert AppConfig.is_production() is False

        assert AppConfig.set_test_mode(AppTestMode.CLIENT_TEST)
        assert AppConfig.is_client_test() is True
        assert AppConfig.is_unit_test() is False
        assert AppConfig.is_production() is False

        AppConfig.set_test_mode(AppTestMode.PRODUCTION)
        assert AppConfig.get_test_mode() == AppTestMode.PRODUCTION
        assert AppConfig.is_client_test() is False
        assert AppConfig.is_unit_test() is False
        assert AppConfig.is_production() is True
