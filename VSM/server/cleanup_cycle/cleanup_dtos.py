from fastapi import HTTPException
from sqlmodel import Session
from datetime import date, timedelta
from datamodel import dtos
from db import db_api
from db.database import Database

class CleanupState:
    dto:dtos.CleanupConfigurationDTO = None

    def __init__(self, dto: dtos.CleanupConfigurationDTO):
        self.dto = dto

    @staticmethod
    def load_by_id(session, cleanup_config_id: int) -> "CleanupState":
        cleanup_config: dtos.CleanupConfigurationDTO = session.get(dtos.CleanupConfigurationDTO, cleanup_config_id)
        if cleanup_config is None:
            raise HTTPException(status_code=404, detail=f"CleanupConfigurationDTO with id:{cleanup_config_id} not found")
        return CleanupState(cleanup_config)
    
    @staticmethod
    def load_by_rootfolder_id(rootfolder_id: int) -> "CleanupState":
        cfg: dtos.CleanupConfigurationDTO = db_api.get_cleanup_configuration_by_rootfolder_id(rootfolder_id)    
        cleanup_config: CleanupState = CleanupState(cfg)
        return cleanup_config
    
    def save_to_db(self, session:Session=None) -> None:
        if session:
            session.add(self.dto)
            session.commit()
            session.refresh(self.dto)
        else:    
            with Session(Database.get_engine()) as session:
                session.add(self.dto)
                session.commit()
                session.refresh(self.dto)

    def is_valid(self) -> bool:
        return self.dto.is_valid()
    
    @property
    def cleanup_progress(self) -> str:
        return self.dto.cleanup_progress
    @cleanup_progress.setter
    def cleanup_progress(self, value: str) -> None:
        self.dto.cleanup_progress = value
    
    def can_start_cleanup_now(self) -> bool:
        # Return true if 
        # cleanup is ready to start at some point
        # and the cleanup_start_date is today or in the past
        # and self.cleanup_progress is INACTIVE or FINISHED
        
        # has valid configuration 
        has_valid_configuration = self.is_valid() and self.dto.cleanup_start_date is not None and self.dto.cleanup_start_date <= date.today() 
        if not has_valid_configuration:
            return False

        has_valid_progress = self.dto.cleanup_progress in [dtos.CleanupProgress.ProgressEnum.INACTIVE.value, dtos.CleanupProgress.ProgressEnum.DONE.value]
        return has_valid_progress
    
    def is_in_cleanup_round(self) -> bool:
        return self.dto.cleanup_progress in [dtos.CleanupProgress.ProgressEnum.RETENTION_REVIEW.value, dtos.CleanupProgress.ProgressEnum.CLEANING.value, dtos.CleanupProgress.ProgressEnum.FINISHING.value]
    
    def is_starting_cleanup_round(self) -> bool:
        return self.dto.cleanup_progress in [dtos.CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW.value]
    
    def can_transition_to(self, new_state: dtos.CleanupProgress.ProgressEnum) -> bool:
        """Check if transition to new_state is valid from current state."""

        # transitions require a valid configuration or that the new state is INACTIVE
        if not (self.is_valid() or new_state == dtos.CleanupProgress.ProgressEnum.INACTIVE):
            return False

        current = dtos.CleanupProgress.ProgressEnum(self.dto.cleanup_progress)
        
        if new_state not in dtos.CleanupProgress.valid_transitions.get(current, []):
            return False

        return True
    
    def transition_to(self, new_state: dtos.CleanupProgress.ProgressEnum) -> bool:
        """
        Transition to a new cleanup progress state.
        
        Args:
            new_state: The state to transition to
            
        Returns:
            tuple[bool, str]: (success, message) - True if transition succeeded, False otherwise
        """
        can_transition = self.can_transition_to(new_state)
        
        if can_transition:
            self.cleanup_progress = new_state.value
            return True
        
        return False
    
    def transition_to_next(self) -> bool:
        """
        Transition to the next default state in the cleanup workflow.
        Follows the primary path: INACTIVE -> STARTED -> CLEANING -> FINISHED -> INACTIVE
        
        Returns:
            tuple[bool, str]: (success, message) - True if transition succeeded, False otherwise
        """
        current = dtos.CleanupProgress.ProgressEnum(self.dto.cleanup_progress)
        next_state = dtos.CleanupProgress.next_natural_state.get(current, None)
        if next_state is None:
            return False
        
        return self.transition_to(next_state)

    def transition_to_inactive(self) -> bool:
        from cleanup_cycle import cleanup_db_actions
        self.dto.cleanup_progress = dtos.CleanupProgress.ProgressEnum.INACTIVE.value
        cleanup_db_actions.deactivate_calendar(self.dto.rootfolder_id)
        return True
    def transition_to_next_round(self) -> bool:
        if not self.dto.cleanup_progress == dtos.CleanupProgress.ProgressEnum.FINISHING:
            raise HTTPException(status_code=400, detail="RootFolder is not in FINISHING state")
        else:
            self.dto.cleanup_start_date = self.dto.cleanup_start_date + timedelta(days=self.dto.cleanupfrequency)
            self.dto.cleanup_progress = dtos.CleanupProgress.ProgressEnum.DONE.value
            return True
