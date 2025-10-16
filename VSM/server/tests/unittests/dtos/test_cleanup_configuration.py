"""
Unit tests for CleanupConfiguration and CleanupProgressEnum
"""
import pytest
from datetime import date
from datamodel.dtos import CleanupConfiguration, CleanupProgressEnum


class TestCleanupProgressEnum:
    """Test CleanupProgressEnum enumeration"""

    def test_enum_values(self):
        """Test that enum has correct values"""
        assert CleanupProgressEnum.INACTIVE.value == "inactive"
        assert CleanupProgressEnum.STARTED.value == "started"
        assert CleanupProgressEnum.CLEANING.value == "cleaning"
        assert CleanupProgressEnum.FINISHED.value == "finished"

    def test_enum_comparison(self):
        """Test enum comparison"""
        assert CleanupProgressEnum.INACTIVE == CleanupProgressEnum.INACTIVE
        assert CleanupProgressEnum.INACTIVE != CleanupProgressEnum.STARTED

    def test_enum_from_string(self):
        """Test creating enum from string value"""
        assert CleanupProgressEnum("inactive") == CleanupProgressEnum.INACTIVE
        assert CleanupProgressEnum("started") == CleanupProgressEnum.STARTED
        assert CleanupProgressEnum("cleaning") == CleanupProgressEnum.CLEANING
        assert CleanupProgressEnum("finished") == CleanupProgressEnum.FINISHED


class TestCleanupConfiguration:
    """Test CleanupConfiguration dataclass"""

    def test_create_default_configuration(self):
        """Test creating a CleanupConfiguration with default progress state"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        assert config.cycletime == 30
        assert config.cleanupfrequency == 90
        assert config.cleanup_start_date == date(2025, 10, 1)
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE

    def test_create_with_explicit_progress(self):
        """Test creating a CleanupConfiguration with explicit progress state"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        assert config.cleanup_progress == CleanupProgressEnum.STARTED

    def test_equality_same_values(self):
        """Test equality comparison with same values"""
        config1 = CleanupConfiguration(30, 90, date(2025, 10, 1), CleanupProgressEnum.INACTIVE)
        config2 = CleanupConfiguration(30, 90, date(2025, 10, 1), CleanupProgressEnum.INACTIVE)
        
        assert config1 == config2

    def test_equality_different_progress(self):
        """Test equality comparison with different progress states"""
        config1 = CleanupConfiguration(30, 90, date(2025, 10, 1), CleanupProgressEnum.INACTIVE)
        config2 = CleanupConfiguration(30, 90, date(2025, 10, 1), CleanupProgressEnum.STARTED)
        
        assert config1 != config2

    def test_equality_different_cycletime(self):
        """Test equality comparison with different cycletime"""
        config1 = CleanupConfiguration(30, 90, date(2025, 10, 1))
        config2 = CleanupConfiguration(60, 90, date(2025, 10, 1))
        
        assert config1 != config2

    def test_equality_different_cleanupfrequency(self):
        """Test equality comparison with different cleanupfrequency"""
        config1 = CleanupConfiguration(30, 90, date(2025, 10, 1))
        config2 = CleanupConfiguration(30, 180, date(2025, 10, 1))
        
        assert config1 != config2

    def test_equality_different_type(self):
        """Test equality comparison with different type"""
        config = CleanupConfiguration(30, 90, date(2025, 10, 1))
        
        assert config != "not a config"
        assert config != 123
        assert config != None


class TestCleanupConfigurationValidation:
    """Test CleanupConfiguration validation methods"""

    def test_valid_configuration_with_frequency(self):
        """Test that configuration is valid when cleanupfrequency and cycletime are set"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        is_valid, message = config.is_valid()
        assert is_valid is True
        assert message == "ok"

    def test_valid_configuration_without_frequency(self):
        """Test that configuration is valid when cleanupfrequency is None"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None
        )
        
        is_valid, message = config.is_valid()
        assert is_valid is True
        assert message == "ok"

    def test_invalid_configuration_frequency_without_cycletime(self):
        """Test that configuration is invalid when cleanupfrequency is set but cycletime is not"""
        config = CleanupConfiguration(
            cycletime=None,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        is_valid, message = config.is_valid()
        assert is_valid is False
        assert "cycletime must be set" in message

    def test_invalid_configuration_frequency_with_zero_cycletime(self):
        """Test that configuration is invalid when cycletime is zero"""
        config = CleanupConfiguration(
            cycletime=0,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        is_valid, message = config.is_valid()
        assert is_valid is False
        assert "cycletime must be set" in message

    def test_invalid_progress_state_without_frequency(self):
        """Test that non-INACTIVE progress requires cleanupfrequency"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        is_valid, message = config.is_valid()
        assert is_valid is False
        assert "cleanup_progress must be INACTIVE when cleanupfrequency is None" in message

    def test_invalid_progress_state_without_cycletime(self):
        """Test that non-INACTIVE progress requires valid cycletime"""
        config = CleanupConfiguration(
            cycletime=0,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        is_valid, message = config.is_valid()
        assert is_valid is False
        # The first validation error should be about cycletime
        assert "cycletime must be set if cleanupfrequency is set" in message

    def test_can_start_cleanup_true(self):
        """Test can_start_cleanup returns True when configuration is valid and frequency is set"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        assert config.can_start_cleanup() is True

    def test_can_start_cleanup_false_no_frequency(self):
        """Test can_start_cleanup returns False when frequency is None"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None
        )
        
        assert config.can_start_cleanup() is False

    def test_can_start_cleanup_false_invalid_config(self):
        """Test can_start_cleanup returns False when configuration is invalid"""
        config = CleanupConfiguration(
            cycletime=0,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        assert config.can_start_cleanup() is False


class TestCleanupConfigurationStateTransitions:
    """Test CleanupConfiguration state transition methods"""

    def test_transition_inactive_to_started_valid(self):
        """Test valid transition from INACTIVE to STARTED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.STARTED)
        assert can_transition is True
        assert message == "ok"

    def test_transition_started_to_cleaning_valid(self):
        """Test valid transition from STARTED to CLEANING"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.CLEANING)
        assert can_transition is True
        assert message == "ok"

    def test_transition_started_to_inactive_valid(self):
        """Test valid transition from STARTED to INACTIVE"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True
        assert message == "ok"

    def test_transition_cleaning_to_finished_valid(self):
        """Test valid transition from CLEANING to FINISHED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.FINISHED)
        assert can_transition is True
        assert message == "ok"

    def test_transition_cleaning_to_inactive_valid(self):
        """Test valid transition from CLEANING to INACTIVE"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True
        assert message == "ok"

    def test_transition_finished_to_inactive_valid(self):
        """Test valid transition from FINISHED to INACTIVE"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True
        assert message == "ok"

    def test_transition_finished_to_started_valid(self):
        """Test valid transition from FINISHED to STARTED (new round)"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.STARTED)
        assert can_transition is True
        assert message == "ok"

    def test_transition_inactive_to_cleaning_invalid(self):
        """Test invalid transition from INACTIVE to CLEANING"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.CLEANING)
        assert can_transition is False
        assert "cannot transition from inactive to cleaning" in message

    def test_transition_started_to_finished_invalid(self):
        """Test invalid transition from STARTED to FINISHED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.FINISHED)
        assert can_transition is False
        assert "cannot transition from started to finished" in message

    def test_transition_finished_to_cleaning_invalid(self):
        """Test invalid transition from FINISHED to CLEANING"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.CLEANING)
        assert can_transition is False
        assert "cannot transition from finished to cleaning" in message

    def test_transition_without_valid_config(self):
        """Test that transition to non-INACTIVE state fails without valid config"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        can_transition, message = config.can_transition_to(CleanupProgressEnum.STARTED)
        assert can_transition is False
        assert "cleanup configuration is not valid or cleanupfrequency is not set" in message

    def test_get_valid_next_states_from_inactive_with_valid_config(self):
        """Test getting valid next states from INACTIVE with valid config"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        next_states = config.get_valid_next_states()
        assert next_states == [CleanupProgressEnum.STARTED]

    def test_get_valid_next_states_from_inactive_without_frequency(self):
        """Test getting valid next states from INACTIVE without frequency (empty list)"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        next_states = config.get_valid_next_states()
        assert next_states == []

    def test_get_valid_next_states_from_started(self):
        """Test getting valid next states from STARTED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        next_states = config.get_valid_next_states()
        assert set(next_states) == {CleanupProgressEnum.CLEANING, CleanupProgressEnum.INACTIVE}

    def test_get_valid_next_states_from_cleaning(self):
        """Test getting valid next states from CLEANING"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        next_states = config.get_valid_next_states()
        assert set(next_states) == {CleanupProgressEnum.FINISHED, CleanupProgressEnum.INACTIVE}

    def test_get_valid_next_states_from_finished(self):
        """Test getting valid next states from FINISHED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        next_states = config.get_valid_next_states()
        assert set(next_states) == {CleanupProgressEnum.INACTIVE, CleanupProgressEnum.STARTED}


class TestCleanupConfigurationCompleteWorkflow:
    """Test complete workflow scenarios"""

    def test_complete_cleanup_cycle(self):
        """Test a complete cleanup cycle: INACTIVE -> STARTED -> CLEANING -> FINISHED -> INACTIVE"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        # Start at INACTIVE
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE
        
        # Transition to STARTED
        can_transition, _ = config.can_transition_to(CleanupProgressEnum.STARTED)
        assert can_transition is True
        config.cleanup_progress = CleanupProgressEnum.STARTED
        
        # Transition to CLEANING
        can_transition, _ = config.can_transition_to(CleanupProgressEnum.CLEANING)
        assert can_transition is True
        config.cleanup_progress = CleanupProgressEnum.CLEANING
        
        # Transition to FINISHED
        can_transition, _ = config.can_transition_to(CleanupProgressEnum.FINISHED)
        assert can_transition is True
        config.cleanup_progress = CleanupProgressEnum.FINISHED
        
        # Transition back to INACTIVE
        can_transition, _ = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True
        config.cleanup_progress = CleanupProgressEnum.INACTIVE
        
        # Should be able to start a new round
        assert config.can_start_cleanup() is True

    def test_abort_cleanup_from_started(self):
        """Test aborting cleanup from STARTED state"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        # Can abort by going back to INACTIVE
        can_transition, _ = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True

    def test_abort_cleanup_from_cleaning(self):
        """Test aborting cleanup from CLEANING state"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        # Can abort by going back to INACTIVE
        can_transition, _ = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True

    def test_start_new_round_from_finished(self):
        """Test starting a new cleanup round directly from FINISHED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        # Can start new round directly
        can_transition, _ = config.can_transition_to(CleanupProgressEnum.STARTED)
        assert can_transition is True


class TestCleanupConfigurationTransitionTo:
    """Test the transition_to method for state changes"""

    def test_transition_to_valid_state(self):
        """Test successful transition to a valid state"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        success, message = config.transition_to(CleanupProgressEnum.STARTED)
        
        assert success is True
        assert "Transitioned to started" in message
        assert config.cleanup_progress == CleanupProgressEnum.STARTED

    def test_transition_to_invalid_state(self):
        """Test failed transition to an invalid state"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        success, message = config.transition_to(CleanupProgressEnum.CLEANING)
        
        assert success is False
        assert "cannot transition from inactive to cleaning" in message
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE  # State unchanged

    def test_transition_to_complete_cycle_using_method(self):
        """Test complete cycle using transition_to method"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        # INACTIVE -> STARTED
        success, msg = config.transition_to(CleanupProgressEnum.STARTED)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.STARTED
        
        # STARTED -> CLEANING
        success, msg = config.transition_to(CleanupProgressEnum.CLEANING)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.CLEANING
        
        # CLEANING -> FINISHED
        success, msg = config.transition_to(CleanupProgressEnum.FINISHED)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.FINISHED
        
        # FINISHED -> INACTIVE
        success, msg = config.transition_to(CleanupProgressEnum.INACTIVE)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE

    def test_transition_to_abort_from_started(self):
        """Test aborting cleanup using transition_to from STARTED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        success, msg = config.transition_to(CleanupProgressEnum.INACTIVE)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE

    def test_transition_to_abort_from_cleaning(self):
        """Test aborting cleanup using transition_to from CLEANING"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        success, msg = config.transition_to(CleanupProgressEnum.INACTIVE)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE

    def test_transition_to_new_round_from_finished(self):
        """Test starting new round using transition_to from FINISHED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        success, msg = config.transition_to(CleanupProgressEnum.STARTED)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.STARTED

    def test_transition_to_without_valid_configuration(self):
        """Test transition_to fails without valid configuration"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        success, msg = config.transition_to(CleanupProgressEnum.STARTED)
        assert success is False
        assert "cleanup configuration is not valid or cleanupfrequency is not set" in msg
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE  # State unchanged

    def test_transition_to_skip_invalid_state(self):
        """Test that transition_to properly rejects skipping states"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        # Cannot go directly from STARTED to FINISHED
        success, msg = config.transition_to(CleanupProgressEnum.FINISHED)
        assert success is False
        assert "cannot transition from started to finished" in msg
        assert config.cleanup_progress == CleanupProgressEnum.STARTED  # State unchanged

    def test_transition_to_same_state(self):
        """Test transition_to behavior when trying to transition to same state"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        # Trying to stay in same state should fail (not in valid transitions)
        success, msg = config.transition_to(CleanupProgressEnum.STARTED)
        assert success is False
        assert config.cleanup_progress == CleanupProgressEnum.STARTED


class TestCleanupConfigurationTransitionToNext:
    """Test the transition_to_next method for automatic state progression"""

    def test_transition_to_next_from_inactive(self):
        """Test transition from INACTIVE to STARTED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        success, message = config.transition_to_next()
        
        assert success is True
        assert "Transitioned to started" in message
        assert config.cleanup_progress == CleanupProgressEnum.STARTED

    def test_transition_to_next_from_started(self):
        """Test transition from STARTED to CLEANING"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.STARTED
        )
        
        success, message = config.transition_to_next()
        
        assert success is True
        assert "Transitioned to cleaning" in message
        assert config.cleanup_progress == CleanupProgressEnum.CLEANING

    def test_transition_to_next_from_cleaning(self):
        """Test transition from CLEANING to FINISHED"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        success, message = config.transition_to_next()
        
        assert success is True
        assert "Transitioned to finished" in message
        assert config.cleanup_progress == CleanupProgressEnum.FINISHED

    def test_transition_to_next_from_finished(self):
        """Test transition from FINISHED back to INACTIVE"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        success, message = config.transition_to_next()
        
        assert success is True
        assert "Transitioned to inactive" in message
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE

    def test_transition_to_next_complete_cycle(self):
        """Test complete cleanup cycle using transition_to_next"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        # INACTIVE -> STARTED
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE
        success, _ = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.STARTED
        
        # STARTED -> CLEANING
        success, _ = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.CLEANING
        
        # CLEANING -> FINISHED
        success, _ = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.FINISHED
        
        # FINISHED -> INACTIVE
        success, _ = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE

    def test_transition_to_next_without_valid_config(self):
        """Test transition_to_next fails from INACTIVE without valid config"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=None,  # No frequency set
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        success, message = config.transition_to_next()
        
        assert success is False
        assert "cleanup configuration is not valid or cleanupfrequency is not set" in message
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE  # State unchanged

    def test_transition_to_next_multiple_rounds(self):
        """Test multiple cleanup rounds using transition_to_next"""
        config = CleanupConfiguration(
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        # First round
        for _ in range(4):  # INACTIVE -> STARTED -> CLEANING -> FINISHED -> INACTIVE
            success, _ = config.transition_to_next()
            assert success is True
        
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE
        
        # Second round should work the same way
        success, _ = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.STARTED


