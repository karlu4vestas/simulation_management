from __future__ import annotations
from typing import TYPE_CHECKING
from enum import Enum
from datetime import date, datetime, timedelta
from http.client import HTTPException
from sqlmodel import SQLModel, Field
from pydantic import BaseModel

if TYPE_CHECKING:
    from cleanup_cycle import cleanup_db_actions


#STARTING_RETENTION_REVIEW  is the only phase where the backend is allowed to mark simulation for cleanup.
#Simulations imported in this phase must postpone possible marked for cleanup to the next cleanup round
class CleanupProgress:
    class ProgressEnum(str, Enum):
        """Enumeration of cleanup round progress states."""
        INACTIVE                    = "inactive"    # No cleanup is active
        STARTING_RETENTION_REVIEW   = "starting_retention_review"      # This is the only phase where the backend is allowed to mark simulation for cleanup.
        RETENTION_REVIEW            = "retention_review"      # Markup phase - users can adjust what simulations will be cleaned. 
        CLEANING                    = "cleaning"    # Actual cleaning is happening
        FINISHING                   = "finish_cleanup_round"    # finish the cleanup round
        DONE                        = "cleanup_is_done"    # Cleanup round is complete, waiting for next round

    # Define valid state transitions
    valid_transitions: dict["CleanupProgress.ProgressEnum", list["CleanupProgress.ProgressEnum"]] = {
        ProgressEnum.INACTIVE: [ProgressEnum.STARTING_RETENTION_REVIEW],
        ProgressEnum.STARTING_RETENTION_REVIEW: [ProgressEnum.RETENTION_REVIEW],
        ProgressEnum.RETENTION_REVIEW: [ProgressEnum.CLEANING, ProgressEnum.INACTIVE],
        ProgressEnum.CLEANING: [ProgressEnum.FINISHING, ProgressEnum.INACTIVE],
        ProgressEnum.FINISHING: [ProgressEnum.DONE, ProgressEnum.INACTIVE],
        ProgressEnum.DONE: [ProgressEnum.INACTIVE, ProgressEnum.STARTING_RETENTION_REVIEW],
    }
    
    # Define the natural progression through cleanup states
    next_natural_state: dict["CleanupProgress.ProgressEnum", "CleanupProgress.ProgressEnum"] = {
        ProgressEnum.INACTIVE: ProgressEnum.STARTING_RETENTION_REVIEW,
        ProgressEnum.STARTING_RETENTION_REVIEW: ProgressEnum.RETENTION_REVIEW,
        ProgressEnum.RETENTION_REVIEW: ProgressEnum.CLEANING,
        ProgressEnum.CLEANING: ProgressEnum.FINISHING,
        ProgressEnum.FINISHING: ProgressEnum.DONE,
        ProgressEnum.DONE: ProgressEnum.STARTING_RETENTION_REVIEW,
    }



# The configuration can be used as follow:
#   a) deactivating cleanup is done by setting cleanupfrequency to None
#   b) activating a cleanup round requires that cleanupfrequency is set and that the cycletime is > 0. 
#        If cleanup_round_start_date is not set then we assume today
#   c) cycletime: is minimum number of days from last modification of a simulation til it can be cleaned
#        It can be set with cleanup is inactive cleanupfrequency is None
#   d) cleanup_progress to describe where the rootfolder is in the cleanup round: 
#      - inactive: going from an activate state to inactive will set the cleanup_start_date to None. 
#                  If inactivate state and cleanupfrequency, cycletime and cleanup_start_date will start the cleanup when the cleanup_start_date is reached.
#      - started:  the markup phase starts then cleanup round starts so that the user can adjust what simulations will be cleaned
#      - cleaning: this is the last phase in which the actual cleaning happens
#      - finished: the cleanup round is finished and we wait for the next round
class CleanupConfigurationBase(SQLModel):
    """Base class for cleanup configuration."""
    rootfolder_id: int              = Field(default=None, foreign_key="rootfolderdto.id")
    cycletime: int                  = Field(default=0)  # days a simulation must be available before cleanup can start. 
    cleanupfrequency: float         = Field(default=0)  # days to next cleanup round. we use float because automatic testing may require setting it to 1 second like 1/(24*60*60) of a day
    cleanup_start_date: date | None = Field(default=None)
    cleanup_progress: str           = Field(default=CleanupProgress.ProgressEnum.INACTIVE.value)

class CleanupConfigurationDTO(CleanupConfigurationBase, table=True):
    """Cleanup configuration as separate table."""
    id: int | None = Field(default=None, primary_key=True)
    
    # Relationship
    #rootfolder: "RootFolderDTO" = Relationship(back_populates="cleanup_config")
    def transition_to_inactive(self) -> bool:
        from cleanup_cycle import cleanup_db_actions
        self.cleanup_progress = CleanupProgress.ProgressEnum.INACTIVE.value
        cleanup_db_actions.deactivate_calendar(self.rootfolder_id)
        return True
    def transition_to_next_round(self) -> bool:
        if not self.cleanup_progress == CleanupProgress.ProgressEnum.FINISHING:
            raise HTTPException(status_code=400, detail="RootFolder is not in FINISHING state")
        else:
            self.cleanup_start_date = self.cleanup_start_date + timedelta(days=self.cleanupfrequency)
            self.cleanup_progress = CleanupProgress.ProgressEnum.DONE.value
            return True


    def __eq__(self, other):
        if not isinstance(other, CleanupConfigurationDTO):
            return False
        return (self.cycletime == other.cycletime and 
                self.cleanupfrequency == other.cleanupfrequency and 
                self.cleanup_start_date == other.cleanup_start_date and
                self.cleanup_progress == other.cleanup_progress)
   
    def is_valid(self) -> bool:
        # has cleanup_frequency and cycle_time been set. 
        # If cleanup_start_date is None then cleanup_progress must be INACTIVE
        is_valid: bool = (self.cleanupfrequency is not None and self.cleanupfrequency > 0) and \
                         (self.cycletime is not None and self.cycletime > 0) and \
                         ((self.cleanup_progress == CleanupProgress.ProgressEnum.INACTIVE.value) \
                          or self.cleanup_start_date is not None)
        return is_valid
    
    def can_start_cleanup_now(self) -> bool:
        # Return true if 
        # cleanup is ready to start at some point
        # and the cleanup_start_date is today or in the past
        # and self.cleanup_progress is INACTIVE or FINISHED
        
        # has valid configuration 
        has_valid_configuration = self.is_valid() and self.cleanup_start_date is not None and self.cleanup_start_date <= date.today() 
        if not has_valid_configuration:
            return False

        has_valid_progress = self.cleanup_progress in [CleanupProgress.ProgressEnum.INACTIVE.value, CleanupProgress.ProgressEnum.DONE.value]
        return has_valid_progress
    
    def is_in_cleanup_round(self) -> bool:
        return self.cleanup_progress in [CleanupProgress.ProgressEnum.RETENTION_REVIEW.value, CleanupProgress.ProgressEnum.CLEANING.value, CleanupProgress.ProgressEnum.FINISHING.value]
    
    def is_starting_cleanup_round(self) -> bool:
        return self.cleanup_progress in [CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW.value]


    def can_transition_to(self, new_state: CleanupProgress.ProgressEnum) -> bool:
        """Check if transition to new_state is valid from current state."""

        # transitions require a valid configuration or that the new state is INACTIVE
        if not (self.is_valid() or new_state == CleanupProgress.ProgressEnum.INACTIVE):
            return False

        current = CleanupProgress.ProgressEnum(self.cleanup_progress)
        
        if new_state not in CleanupProgress.valid_transitions.get(current, []):
            return False

        return True
    
    def transition_to(self, new_state: CleanupProgress.ProgressEnum) -> bool:
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
        current = CleanupProgress.ProgressEnum(self.cleanup_progress)
        next_state = CleanupProgress.next_natural_state.get(current, None)
        if next_state is None:
            return False
        
        return self.transition_to(next_state)
