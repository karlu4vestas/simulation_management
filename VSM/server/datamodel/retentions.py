from enum import Enum
from bisect import bisect_left
from dataclasses import dataclass
from typing import Literal, Optional
from datetime import date, datetime, timedelta
from sqlmodel import Field, SQLModel, Session
from dataclasses import dataclass
from datamodel import dtos
from datamodel.dtos import ExternalRetentionTypes, Retention, RetentionTypeDTO
from cleanup import cleanup_dtos
from app.clock import SystemClock


class RetentionCalculator:
    # use ids for __init__ in order to avoid circular import due to RootFolderDTO
    def __init__(self, rootfolder_id: int, cleanup_config_id: int, session:Session):
        from db.db_api import read_pathprotections, read_rootfolder_retentiontypes_dict # avoid circular import
        # info for path retentions
        self.path_retention_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retentiontypes_dict(rootfolder_id)
        self.path_retention_id:int = self.path_retention_dict["path"].id if "path" in self.path_retention_dict else 0
        self.default_path_protection_id: int = 0 # @TODO wil this be used?

        #self.match_prefix_id = make_prefix_id_matcher(protections)
        protections:list[dtos.PathProtectionDTO] = read_pathprotections(rootfolder_id)
        self.sorted_protections: list[tuple[str, int]] = [(dto.path.lower().replace('\\', '/').rstrip('/'), dto.id) for dto in protections]

        self.sorted_protections: list[tuple[str, int]] = sorted(
            self.sorted_protections,
            key=lambda item: item[0].count('/'),
            reverse=True
        )


        # Info for all other retentions
        
        if not cleanup_config_id or cleanup_config_id <= 0:
            raise ValueError("The cleanup_config_id:{cleanup_config_id} must be valid")
        
        cleanup_state: cleanup_dtos.CleanupState = cleanup_dtos.CleanupState.load_by_id(session, cleanup_config_id) 
        retention_type_dict: dict[str, RetentionTypeDTO] = read_rootfolder_retentiontypes_dict(rootfolder_id)

        if not retention_type_dict or not cleanup_state.is_valid():
            raise ValueError("cleanup_round_start_date, at least one numeric retention type and lead_time must be set for RetentionCalculator to work")

        # It is on the one hand practical to make a first configuration of retention without starting a cleanup round, if the user desires this
        # but on the other hand the RetentionCalculator requires a start_date to be able to calculate retentions
        # If the cleanup_state can be used to start a cleanup round then use its start date
        # If the progress is INACTIVE and no start_date is set, we set the cleanup_round_start_date to SystemClock.now(). 
        # Notice that no retention will be marked with cleanup progress in CleanupProgress.ProgressEnum.INACTIVE
        start_date = None
        if cleanup_state.is_valid() and cleanup_state.dto.start_date is not None:
            start_date = cleanup_state.dto.start_date
        elif cleanup_state.dto.progress == dtos.CleanupProgress.Progress.INACTIVE.value:
            start_date = SystemClock.now()
        else:
            raise ValueError(f"The RetentionCalculator cannot work with the cleanup configuration:{cleanup_state}")
        self.cleanup_round_start_date    = start_date

        self.leadtimedelta              = timedelta(days=cleanup_state.dto.lead_time)

        self.retention_type_str_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retentiontypes_dict(rootfolder_id)
        self.retention_type_id_dict      = {retention.id: retention for retention in self.retention_type_str_dict.values()}
        self.path_retention_id           = self.retention_type_str_dict["path"].id   if self.retention_type_str_dict.get("path", None) is not None else 0  
        self.marked_retention_id         = self.retention_type_str_dict["marked"].id if self.retention_type_str_dict.get("marked", None) is not None else 0  
        self.undefined_retention_id      = self.retention_type_str_dict["?"].id      if self.retention_type_str_dict.get("?", None) is not None else 0  
        self.progress            = cleanup_state.dto.progress
        self.is_in_cleanup_round         = cleanup_state.is_in_cleanup_round()
        self.is_starting_cleanup_round   = cleanup_state.is_starting_cleanup_round()

        # create numeric retention values sorted by days_to_cleanup
        numeric_retention_types:list[RetentionTypeDTO]      = sorted([retention for key,retention in retention_type_dict.items() if retention.days_to_cleanup is not None ], key=lambda r: r.days_to_cleanup)

        self.numeric_retention_id_dict   = {retention.id: retention   for retention in numeric_retention_types}
        self.numeric_retention_ids       = [retention.id              for retention in numeric_retention_types]
        self.numeric_retention_durations = [retention.days_to_cleanup for retention in numeric_retention_types]

    def is_numeric(self, retention_id:int) -> bool:
        return retention_id in self.numeric_retention_id_dict   

    def is_valid(self, retention:Retention) -> bool:
        if retention.retention_id is None or self.retention_type_str_dict.get(retention.retention_id, None) is None:
            #the retention_id is not defined or is invalid
            return False
        elif self.is_numeric(retention.retention_id):
            return retention.expiration_date is not None
        else:
            return True

    def is_endstage(self, retention_id:int) -> bool:
        retentiontype:RetentionTypeDTO = self.retention_type_id_dict.get(retention_id, None)
        return retentiontype is not None and retentiontype.is_endstage 

    def get_endstage_retentions(self) -> list[RetentionTypeDTO]:
        return [retentiontype for retentiontype in self.retention_type_id_dict.values() if retentiontype.is_endstage]

    # The usecase is that the "end user" has selected a retention relative to the cleanup configuration'
    # if non numeric retention then set expiration_date to None
    # if numeric retention then set expiration_date to cleanup_round_start_date + days_to_cleanup of the retention type
    def adjust_expiration_date_from_cleanup_configuration_and_retentiontype(self, retention: Retention) -> Retention:
        retentiontype = self.numeric_retention_id_dict.get(retention.retention_id, None)
        if retentiontype is None: # not a numeric retention
            retention.expiration_date = None
        else:
            retention.expiration_date = self.cleanup_round_start_date + timedelta(days = retentiontype.days_to_cleanup)
        return retention


    # adjust expiration_date before retention_id using the cleanup_configuration and modified_date
    #   - This is what you what when updating the simulation from a scan of its metadata
    # if modified_date is None then the expiration_date is only change for non numeric retentions. for numeric retention we adjust the retention_id
    #
    # if non numeric retention then set expiration_date to None
    # if numeric retention then 
    #   use the modified_date to update expiration_date if it will result in longer retention (expiration_date)
    #   update numeric retention_id to the new expiration date. The retention_id is calculated; even if the expiration date did not change to be sure there is no inconsistency
    def adjust_from_cleanup_configuration_and_modified_date(self, retention:Retention, modified_date:datetime, new_modified_date:datetime=None) -> Retention:
        if modified_date is None:# must be a new simulation
            if new_modified_date is not None: 
                #if so the use the new modified date
                modified_date = new_modified_date
            
            if  retention.retention_id is None:
                #no existing valid retention_id so we must set the retention to undefined
                retention.retention_id = self.undefined_retention_id  
        elif new_modified_date is not None and new_modified_date != modified_date:
            # so we have an existing retention and a new modified date:
            #    => the retention must be recalculated unless it is path protected
            modified_date = new_modified_date
            if retention.retention_id != self.path_retention_id:
                retention.retention_id = self.undefined_retention_id #then is will be schduled to cleanup sooner or later

        if retention.retention_id is not None and not self.is_numeric(retention.retention_id) and retention.retention_id != self.undefined_retention_id: 
            # the retention is endstage or path retention so do not change the retention_id
            retention.expiration_date = None
        else: # so it is a numeric or unknown retention                
            if modified_date is not None:
                retention.expiration_date = (modified_date + self.leadtimedelta) if retention.expiration_date is None else max(retention.expiration_date, modified_date + self.leadtimedelta)

            if retention.expiration_date is None:
                raise ValueError("retention.expiration_date is None in RetentionCalculator adjust_from_cleanup_configuration_and_modified_date")
            elif self.cleanup_round_start_date != None: 
                if self.is_starting_cleanup_round: #this is the phase where all retention are adjusted according to their expiration_date
                    days_to_expiration = (retention.expiration_date - self.cleanup_round_start_date).days
                    # find first index where retention_duration[idx] >= days_until_expiration
                    idx = bisect_left(self.numeric_retention_durations, days_to_expiration)

                    # if days_until_expiration is greater than every threshold, return last index
                    retention.retention_id = self.numeric_retention_ids[idx] if idx < len(self.numeric_retention_durations) else self.numeric_retention_ids[len(self.numeric_retention_durations) - 1]
                elif retention.retention_id is None or retention.retention_id == self.marked_retention_id:
                    # The user controls the retention so outside the progress state self.is_starting_cleanup_round we must only change numeric retentions that are
                    #    - None
                    #    - or marked for clean up. because the user might change the modified_date of a marked retention during the cleanup round.
                    days_to_expiration = (retention.expiration_date - self.cleanup_round_start_date).days
                    # find first index where retention_duration[idx] >= days_until_expiration
                    idx = bisect_left(self.numeric_retention_durations, days_to_expiration)

                    # if days_until_expiration is greater than every threshold, return last index
                    retention_id = self.numeric_retention_ids[idx] if idx < len(self.numeric_retention_durations) else self.numeric_retention_ids[len(self.numeric_retention_durations) - 1]

                    if retention.retention_id == self.marked_retention_id and retention_id != self.marked_retention_id:
                        # special case: the simulation is in the retention_review or cleaning phase but the user has modified the retention,
                        # we must assign the new retention because modifying a simulation during the cleanup round is also a way of telling that the simulation should not be cleaned yet
                        retention.retention_id = retention_id
                    elif retention_id == self.marked_retention_id :
                        # The new retention is about to be marked which is not OK. To handle this we must pick next retention after marked.
                        retention.retention_id = self.get_retention_id_after_marked()
                    else:
                        retention.retention_id = retention_id 

            #if retention.retention_id == self.marked_retention_id:
            #    print("Warning: retention_id is set to 'marked' in RetentionCalculator adjust_from_cleanup_configuration_and_modified_date")

        return retention
    def get_retention_id_after_marked(self) -> Optional[RetentionTypeDTO]:
        # skip marked at index 0. 
        # This works because 1) the "marked" retention is mandatory for the cleanup solution and has days_to_cleanup = 0 
        # and 2) numeric_retention_ids is sorted in ascending days_to_cleanup
        return self.numeric_retention_ids[1]


    def to_internal_type_id(self, external_retention: ExternalRetentionTypes) -> int|None:
        internal_retention_id:int|None = None
        if external_retention is None:
            raise ValueError(f"External retention type is None")
        elif external_retention == ExternalRetentionTypes.NUMERIC:
            # if numeric then assign undefined retention so the its numeric retention gets calculated
            internal_retention_id = self.undefined_retention_id
        else:
            # this lookup work due because the all retentions type string names, external and internal, are guaranteed to be the same string due to the Literal type RetentionName
            # the default None is defensive programming; this should never happen because the dict' keys are a superset of the ExternalRetentionTypes values
            internal_retention:RetentionTypeDTO = self.retention_type_str_dict.get(external_retention.value, None)
            internal_retention_id = internal_retention.id if internal_retention else None
        return internal_retention_id

    # returns: Retention(retention_id, pathprotection_id) or None if not found
    #it returns the id to the PathProtectionDTO and prioritizes the most specific path (most segments) if any.
    def match(self, path:str) -> Optional[Retention]:
        """
        Returns a function that, given a path, returns the id of the longest
        protection that is a prefix of the path (with '/' boundary) or None.
        """
        path = (path or "").rstrip('/').lower().replace('\\', '/')
        for pat, pid in self.sorted_protections:  # short-circuits on first (longest) match
            if path == pat or path.startswith(pat + "/"):  # avoids "R1" matching "R10/..."
                return Retention( self.path_retention_id, pid)
        return None

    def calculate_retention_from_scan(self, db_retention: Retention, db_modified_date: date, sim_external_retention, sim_modified_date: date, folder_path: str) -> tuple[Retention, date]:
        """
        Calculate the retention and modified_date for a folder based on scanned simulation data.
        This consolidates all the complex retention priority logic when updating from a scan.
        
        Priority order:
        1. Path protection (highest priority)
        2. Simulation's external retention (if endstage: clean, issue, missing)
        3. Existing DB retention (if not endstage, keep it)
        4. Calculate numeric retention based on modified_date
        
        Args:
            db_retention: Current retention from database
            db_modified_date: Current modified date from database
            sim_external_retention: External retention type from scanned simulation (can be None)
            sim_modified_date: Modified date from scanned simulation
            folder_path: Path for checking path protection
            
        Returns:
            Tuple of (new_retention, new_modified_date)
        """
        from datamodel.retentions import ExternalRetentionTypes  # avoid circular import if needed
        
        # Initialize with existing values
        new_retention = Retention(
            retention_id=db_retention.retention_id,
            path_protection_id=db_retention.path_protection_id,
            expiration_date=db_retention.expiration_date
        )
        new_modified_date = db_modified_date
        
        # Convert external retention to internal type ID (if provided)
        sim_retention_id = self.to_internal_type_id(sim_external_retention) if sim_external_retention is not None else None
        
        # Check for path protection (highest priority)
        path_retention = self.match(folder_path)
        
        # Apply retention priority logic
        if path_retention is not None:
            # Path retention takes priority over all other retentions
            new_retention = path_retention
        elif sim_retention_id is not None and self.is_endstage(sim_retention_id):
            # Simulation has an endstage retention (clean, issue, or missing) - apply it
            new_retention = Retention(sim_retention_id)
        elif db_retention.retention_id is not None and self.is_endstage(db_retention.retention_id):  #@TODO fishy . Have to review the logic
            # DB retention is in endstage 

            # This means the newly scanned simulation is in a non-endstage state
            # Reset to None so retention_calculator can calculate the correct retention
            new_retention = Retention(retention_id=None)
        else:
            # Keep existing retention (pass - already initialized with db_retention)
            pass
        
        # Handle modified_date changes and calculate numeric retentions
        if db_modified_date != sim_modified_date:
            # Modified date changed - recalculate retention with new date
            new_retention = self.adjust_from_cleanup_configuration_and_modified_date(
                new_retention, db_modified_date, sim_modified_date
            )
            new_modified_date = sim_modified_date
        else:
            # Modified date unchanged - evaluate retention state with current date
            new_retention = self.adjust_from_cleanup_configuration_and_modified_date(
                new_retention, db_modified_date
            )
        
        return new_retention, new_modified_date