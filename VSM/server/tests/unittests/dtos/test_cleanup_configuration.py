"""
Unit tests for CleanupConfigurationDTO and CleanupProgressEnum
"""
import pytest
from datetime import date
from datamodel.dtos import CleanupConfigurationDTO, CleanupProgressEnum, RootFolderDTO


@pytest.fixture
def test_rootfolder(test_session):
    """Create a test rootfolder for CleanupConfigurationDTO tests."""
    rootfolder = RootFolderDTO(simulationdomain_id=1, owner="test", path="/test")
    test_session.add(rootfolder)
    test_session.commit()
    test_session.refresh(rootfolder)
    return rootfolder


class TestCleanupProgressEnum:
    """Test CleanupProgressEnum enumeration"""

    def test_enum_values(self, test_session, test_rootfolder):
        """Test that enum has correct values"""
        assert CleanupProgressEnum.INACTIVE.value == "inactive"
        assert CleanupProgressEnum.RETENTION_REVIEW.value == "retention_review"
        assert CleanupProgressEnum.CLEANING.value == "cleaning"
        assert CleanupProgressEnum.FINISHED.value == "finished"

    def test_enum_comparison(self, test_session, test_rootfolder):
        """Test enum comparison"""
        assert CleanupProgressEnum.INACTIVE == CleanupProgressEnum.INACTIVE
        assert CleanupProgressEnum.INACTIVE != CleanupProgressEnum.RETENTION_REVIEW

    def test_enum_from_string(self, test_session, test_rootfolder):
        """Test creating enum from string value"""
        assert CleanupProgressEnum("inactive") == CleanupProgressEnum.INACTIVE
        assert CleanupProgressEnum("retention_review") == CleanupProgressEnum.RETENTION_REVIEW
        assert CleanupProgressEnum("cleaning") == CleanupProgressEnum.CLEANING
        assert CleanupProgressEnum("finished") == CleanupProgressEnum.FINISHED


class TestCleanupConfigurationDTO:
    """Test CleanupConfigurationDTO dataclass"""

    def test_create_default_configuration(self, test_session, test_rootfolder):
        """Test creating a CleanupConfigurationDTO with default progress state"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        test_session.add(config)
        test_session.commit()
        test_session.refresh(config)
        
        assert config.cycletime == 30
        assert config.cleanupfrequency == 90
        assert config.cleanup_start_date == date(2025, 10, 1)
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value

    def test_create_with_explicit_progress(self, test_session, test_rootfolder):
        """Test creating a CleanupConfigurationDTO with explicit progress state"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW.value
        )
        test_session.add(config)
        test_session.commit()
        test_session.refresh(config)
        
        assert config.cleanup_progress == CleanupProgressEnum.RETENTION_REVIEW.value

    def test_equality_same_values(self, test_session, test_rootfolder):
        """Test equality comparison with same values"""
        config1 = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE.value
        )
        test_session.add(config1)
        test_session.commit()
        test_session.refresh(config1)
        
        config2 = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE.value
        )
        
        # Compare attributes since these are different objects
        assert config1.cycletime == config2.cycletime
        assert config1.cleanupfrequency == config2.cleanupfrequency
        assert config1.cleanup_progress == config2.cleanup_progress

    def test_equality_different_progress(self, test_session, test_rootfolder):
        """Test equality comparison with different progress states"""
        config1 = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE.value
        )
        config2 = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW.value
        )
        
        assert config1.cleanup_progress != config2.cleanup_progress

    def test_equality_different_cycletime(self, test_session, test_rootfolder):
        """Test equality comparison with different cycletime"""
        config1 = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        config2 = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=60,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        assert config1.cycletime != config2.cycletime

    def test_equality_different_cleanupfrequency(self, test_session, test_rootfolder):
        """Test equality comparison with different cleanupfrequency"""
        config1 = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        config2 = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=180,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        assert config1.cleanupfrequency != config2.cleanupfrequency

    def test_equality_different_type(self, test_session, test_rootfolder):
        """Test equality comparison with different type"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        assert config != "not a config"
        assert config != 123
        assert config != None


class TestCleanupConfigurationDTOValidation:
    """Test CleanupConfigurationDTO validation methods"""

    def test_valid_configuration_with_frequency(self, test_session, test_rootfolder):
        """Test that configuration is valid when cleanupfrequency and cycletime are set"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        is_valid = config.is_valid()
        assert is_valid is True

    def test_valid_configuration_without_frequency(self, test_session, test_rootfolder):
        """Test that configuration is valid when cleanupfrequency is None"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None
        )

        is_valid = config.is_valid()
        assert is_valid is False


    def test_invalid_configuration_frequency_without_cycletime(self, test_session, test_rootfolder):
        """Test that configuration is invalid when cleanupfrequency is set but cycletime is not"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=None,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        is_valid = config.is_valid()
        assert is_valid is False

    def test_invalid_configuration_frequency_with_zero_cycletime(self, test_session, test_rootfolder):
        """Test that configuration is invalid when cycletime is zero"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=0,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        is_valid = config.is_valid()
        assert is_valid is False

    def test_invalid_progress_state_without_frequency(self, test_session, test_rootfolder):
        """Test that non-INACTIVE progress requires cleanupfrequency"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW.value
        )
        
        is_valid = config.is_valid()
        assert is_valid is False

    def test_invalid_progress_state_without_cycletime(self, test_session, test_rootfolder):
        """Test that non-INACTIVE progress requires valid cycletime"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=0,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW.value
        )
        
        is_valid = config.is_valid()
        assert is_valid is False


    def test_can_start_cleanup_true(self, test_session, test_rootfolder):
        """Test can_start_cleanup returns True when configuration is valid and frequency is set"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        assert config.is_ready_to_start_cleanup() is True

    def test_can_start_cleanup_false_no_frequency(self, test_session, test_rootfolder):
        """Test can_start_cleanup returns False when frequency is None"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None
        )
        
        assert config.is_ready_to_start_cleanup() is False

    def test_can_start_cleanup_false_invalid_config(self, test_session, test_rootfolder):
        """Test can_start_cleanup returns False when configuration is invalid"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=0,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        assert config.is_ready_to_start_cleanup() is False


class TestCleanupConfigurationDTOStateTransitions:
    """Test CleanupConfigurationDTO state transition methods"""

    def test_transition_inactive_to_started_valid(self, test_session, test_rootfolder):
        """Test valid transition from INACTIVE to STARTED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        can_transition = config.can_transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        assert can_transition is True

    def test_transition_started_to_cleaning_valid(self, test_session, test_rootfolder):
        """Test valid transition from STARTED to CLEANING"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW
        )

        can_transition = config.can_transition_to(CleanupProgressEnum.CLEANING)
        assert can_transition is True

    def test_transition_started_to_inactive_valid(self, test_session, test_rootfolder):
        """Test valid transition from STARTED to INACTIVE"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW
        )
        
        can_transition = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True

    def test_transition_cleaning_to_finished_valid(self, test_session, test_rootfolder):
        """Test valid transition from CLEANING to FINISHED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        can_transition = config.can_transition_to(CleanupProgressEnum.FINISHED)
        assert can_transition is True

    def test_transition_cleaning_to_inactive_valid(self, test_session, test_rootfolder):
        """Test valid transition from CLEANING to INACTIVE"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        can_transition = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True

    def test_transition_finished_to_inactive_valid(self, test_session, test_rootfolder):
        """Test valid transition from FINISHED to INACTIVE"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        can_transition = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True

    def test_transition_finished_to_started_valid(self, test_session, test_rootfolder):
        """Test valid transition from FINISHED to STARTED (new round)"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )

        can_transition = config.can_transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        assert can_transition is True

    def test_transition_inactive_to_cleaning_invalid(self, test_session, test_rootfolder):
        """Test invalid transition from INACTIVE to CLEANING"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        can_transition = config.can_transition_to(CleanupProgressEnum.CLEANING)
        assert can_transition is False

    def test_transition_started_to_finished_invalid(self, test_session, test_rootfolder):
        """Test invalid transition from STARTED to FINISHED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW
        )
        
        can_transition = config.can_transition_to(CleanupProgressEnum.FINISHED)
        assert can_transition is False

    def test_transition_finished_to_cleaning_invalid(self, test_session, test_rootfolder):
        """Test invalid transition from FINISHED to CLEANING"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )

        can_transition = config.can_transition_to(CleanupProgressEnum.CLEANING)
        assert can_transition is False

    def test_transition_without_valid_config(self, test_session, test_rootfolder):
        """Test that transition to non-INACTIVE state fails without valid config"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        can_transition = config.can_transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        assert can_transition is False


    def test_get_valid_next_states_from_inactive_with_valid_config(self, test_session, test_rootfolder):
        """Test getting valid next states from INACTIVE with valid config"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        next_states = config.get_valid_next_states()
        assert next_states == [CleanupProgressEnum.RETENTION_REVIEW]

    def test_get_valid_next_states_from_inactive_without_frequency(self, test_session, test_rootfolder):
        """Test getting valid next states from INACTIVE without frequency (empty list)"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        next_states = config.get_valid_next_states()
        assert next_states == []

    def test_get_valid_next_states_from_started(self, test_session, test_rootfolder):
        """Test getting valid next states from STARTED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW
        )
        
        next_states = config.get_valid_next_states()
        assert set(next_states) == {CleanupProgressEnum.CLEANING, CleanupProgressEnum.INACTIVE}

    def test_get_valid_next_states_from_cleaning(self, test_session, test_rootfolder):
        """Test getting valid next states from CLEANING"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        next_states = config.get_valid_next_states()
        assert set(next_states) == {CleanupProgressEnum.FINISHED, CleanupProgressEnum.INACTIVE}

    def test_get_valid_next_states_from_finished(self, test_session, test_rootfolder):
        """Test getting valid next states from FINISHED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        next_states = config.get_valid_next_states()
        assert set(next_states) == {CleanupProgressEnum.INACTIVE, CleanupProgressEnum.RETENTION_REVIEW}


class TestCleanupConfigurationDTOCompleteWorkflow:
    """Test complete workflow scenarios"""

    def test_complete_cleanup_cycle(self, test_session, test_rootfolder):
        """Test a complete cleanup cycle: INACTIVE -> STARTED -> CLEANING -> FINISHED -> INACTIVE"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        # Start at INACTIVE
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value
        
        # Transition to STARTED
        can_transition = config.can_transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        assert can_transition is True
        config.cleanup_progress = CleanupProgressEnum.RETENTION_REVIEW.value
        
        # Transition to CLEANING
        can_transition = config.can_transition_to(CleanupProgressEnum.CLEANING)
        assert can_transition is True
        config.cleanup_progress = CleanupProgressEnum.CLEANING.value
        
        # Transition to FINISHED
        can_transition = config.can_transition_to(CleanupProgressEnum.FINISHED)
        assert can_transition is True
        config.cleanup_progress = CleanupProgressEnum.FINISHED.value
        
        # Transition back to INACTIVE
        can_transition = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True
        config.cleanup_progress = CleanupProgressEnum.INACTIVE.value
        
        # Should be able to start a new round
        assert config.is_ready_to_start_cleanup() is True

    def test_abort_cleanup_from_started(self, test_session, test_rootfolder):
        """Test aborting cleanup from STARTED state"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW
        )
        
        # Can abort by going back to INACTIVE
        can_transition = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True

    def test_abort_cleanup_from_cleaning(self, test_session, test_rootfolder):
        """Test aborting cleanup from CLEANING state"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        # Can abort by going back to INACTIVE
        can_transition = config.can_transition_to(CleanupProgressEnum.INACTIVE)
        assert can_transition is True

    def test_start_new_round_from_finished(self, test_session, test_rootfolder):
        """Test starting a new cleanup round directly from FINISHED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        # Can start new round directly
        can_transition = config.can_transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        assert can_transition is True


class TestCleanupConfigurationDTOTransitionTo:
    """Test the transition_to method for state changes"""

    def test_transition_to_valid_state(self, test_session, test_rootfolder):
        """Test successful transition to a valid state"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        success = config.transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.RETENTION_REVIEW.value

    def test_transition_to_invalid_state(self, test_session, test_rootfolder):
        """Test failed transition to an invalid state"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )

        success = config.transition_to(CleanupProgressEnum.CLEANING)
        assert success is False
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value  # State unchanged

    def test_transition_to_complete_cycle_using_method(self, test_session, test_rootfolder):
        """Test complete cycle using transition_to method"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        # INACTIVE -> STARTED
        success = config.transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.RETENTION_REVIEW.value
        
        # STARTED -> CLEANING
        success = config.transition_to(CleanupProgressEnum.CLEANING)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.CLEANING.value
        
        # CLEANING -> FINISHED
        success = config.transition_to(CleanupProgressEnum.FINISHED)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.FINISHED.value
        
        # FINISHED -> INACTIVE
        success = config.transition_to(CleanupProgressEnum.INACTIVE)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value

    def test_transition_to_abort_from_started(self, test_session, test_rootfolder):
        """Test aborting cleanup using transition_to from STARTED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW
        )
        
        success = config.transition_to(CleanupProgressEnum.INACTIVE)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value

    def test_transition_to_abort_from_cleaning(self, test_session, test_rootfolder):
        """Test aborting cleanup using transition_to from CLEANING"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )
        
        success = config.transition_to(CleanupProgressEnum.INACTIVE)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value

    def test_transition_to_new_round_from_finished(self, test_session, test_rootfolder):
        """Test starting new round using transition_to from FINISHED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        success = config.transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.RETENTION_REVIEW.value

    def test_transition_to_without_valid_configuration(self, test_session, test_rootfolder):
        """Test transition_to fails without valid configuration"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=None,
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        success = config.transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        assert success is False
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value  # State unchanged

    def test_transition_to_skip_invalid_state(self, test_session, test_rootfolder):
        """Test that transition_to properly rejects skipping states"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW
        )
        
        # Cannot go directly from STARTED to FINISHED
        success = config.transition_to(CleanupProgressEnum.FINISHED)
        assert success is False
        assert config.cleanup_progress == CleanupProgressEnum.RETENTION_REVIEW.value  # State unchanged

    def test_transition_to_same_state(self, test_session, test_rootfolder):
        """Test transition_to behavior when trying to transition to same state"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW
        )
        
        # Trying to stay in same state should fail (not in valid transitions)
        success = config.transition_to(CleanupProgressEnum.RETENTION_REVIEW)
        assert success is False
        assert config.cleanup_progress == CleanupProgressEnum.RETENTION_REVIEW.value


class TestCleanupConfigurationDTOTransitionToNext:
    """Test the transition_to_next method for automatic state progression"""

    def test_transition_to_next_from_inactive(self, test_session, test_rootfolder):
        """Test transition from INACTIVE to STARTED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        success = config.transition_to_next()
        
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.RETENTION_REVIEW.value

    def test_transition_to_next_from_started(self, test_session, test_rootfolder):
        """Test transition from STARTED to CLEANING"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.RETENTION_REVIEW
        )
        
        success = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.CLEANING.value

    def test_transition_to_next_from_cleaning(self, test_session, test_rootfolder):
        """Test transition from CLEANING to FINISHED"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.CLEANING
        )

        success = config.transition_to_next()

        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.FINISHED.value

    def test_transition_to_next_from_finished(self, test_session, test_rootfolder):
        """Test transition from FINISHED back to INACTIVE"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1),
            cleanup_progress=CleanupProgressEnum.FINISHED
        )
        
        success = config.transition_to_next()
        
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value

    def test_transition_to_next_complete_cycle(self, test_session, test_rootfolder):
        """Test complete cleanup cycle using transition_to_next"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        # INACTIVE -> STARTED
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value
        success= config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.RETENTION_REVIEW.value
        
        # STARTED -> CLEANING
        success = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.CLEANING.value
        
        # CLEANING -> FINISHED
        success = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.FINISHED.value
        
        # FINISHED -> INACTIVE
        success = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value

    def test_transition_to_next_without_valid_config(self, test_session, test_rootfolder):
        """Test transition_to_next fails from INACTIVE without valid config"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=None,  # No frequency set
            cleanup_start_date=None,
            cleanup_progress=CleanupProgressEnum.INACTIVE
        )
        
        success = config.transition_to_next()
        
        assert success is False
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value  # State unchanged

    def test_transition_to_next_multiple_rounds(self, test_session, test_rootfolder):
        """Test multiple cleanup rounds using transition_to_next"""
        config = CleanupConfigurationDTO(
            rootfolder_id=test_rootfolder.id,
            cycletime=30,
            cleanupfrequency=90,
            cleanup_start_date=date(2025, 10, 1)
        )
        
        # First round
        for _ in range(4):  # INACTIVE -> STARTED -> CLEANING -> FINISHED -> INACTIVE
            success = config.transition_to_next()
            assert success is True
        
        assert config.cleanup_progress == CleanupProgressEnum.INACTIVE.value
        
        # Second round should work the same way
        succes = config.transition_to_next()
        assert success is True
        assert config.cleanup_progress == CleanupProgressEnum.RETENTION_REVIEW.value


