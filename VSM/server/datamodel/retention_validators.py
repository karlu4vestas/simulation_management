from bisect import bisect_left
from typing import Optional
from datetime import date, timedelta
from datamodel.dtos import CleanupProgress, ExternalRetentionTypes, Retention 
from datamodel.dtos import CleanupConfigurationDTO, RetentionTypeDTO, PathProtectionDTO

#ensure consistency of retentions
#  
class RetentionCalculator:
    def __init__(self, retention_type_dict: dict[str, RetentionTypeDTO], cleanup_config: CleanupConfigurationDTO):
        if not retention_type_dict or not cleanup_config.is_valid():
            raise ValueError("cleanup_round_start_date, at least one numeric retention type and cycletime must be set for RetentionCalculator to work")

        # It is on the one hand practical to make a first configuration of retention without starting a cleanup round, if the user desires this
        # but on the other hand the RetentionCalculator requires a cleanup_start_date to be able to calculate retentions
        # If the cleanup_config can be used to start a cleanup round then use its start date
        # If the cleanup_progress is INACTIVE and no cleanup_start_date is set, we set the cleanup_round_start_date to today. 
        # Notice that no retention will be marked with cleanup progress in CleanupProgress.ProgressEnum.INACTIVE
        start_date = None
        if cleanup_config.is_valid() and cleanup_config.cleanup_start_date is not None:
            start_date = cleanup_config.cleanup_start_date
        elif cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.INACTIVE.value:
            start_date = date.today()
        else:
            raise ValueError(f"The RetentionCalculator cannot work with the cleanup configuration:{cleanup_config}")
        self.cleanup_round_start_date    = start_date

        self.cycletimedelta              = timedelta(days=cleanup_config.cycletime)

        self.retention_type_str_dict     = retention_type_dict
        self.retention_type_id_dict      = {retention.id: retention for retention in self.retention_type_str_dict.values()}
        self.path_retention_id           = self.retention_type_str_dict["path"].id if self.retention_type_str_dict.get("path", None) is not None else 0  
        self.marked_retention_id         = self.retention_type_str_dict["marked"].id if self.retention_type_str_dict.get("marked", None) is not None else 0  
        self.is_in_cleanup_round         = cleanup_config.is_in_cleanup_round()
        self.is_starting_cleanup_round   = cleanup_config.is_starting_cleanup_round()

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

    # adjust the expiration_date using the cleanup_configuration and retentiontype 
    # This is what you what when updating the simulations retentiontype using the webclient
    #
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
    def adjust_from_cleanup_configuration_and_modified_date(self, retention:Retention, modified_date:date) -> Retention:

        if retention.retention_id is not None and not self.is_numeric(retention.retention_id): 
            # the retention is endstage or path retention so do not change the retention_id
            retention.expiration_date = None
        else: # so it is a a numeric or unknown retention

            if modified_date is not None:
                retention.expiration_date = (modified_date + self.cycletimedelta) if retention.expiration_date is None else max(retention.expiration_date, modified_date + self.cycletimedelta)

            if retention.expiration_date is None:
                raise ValueError("retention.expiration_date is None in RetentionCalculator adjust_from_cleanup_configuration_and_modified_date")
            elif self.is_starting_cleanup_round: #this is the phase where all retention are adjusted according to their expiration_date
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

#ensure consistency of path retentions
class PathProtectionEngine:
    sorted_protections:list[tuple[str, int]]
    def __init__(self, protections: list[PathProtectionDTO], path_retention_id:int, default_path_protection_id: int = 0):
        #self.match_prefix_id = make_prefix_id_matcher(protections)
        self.sorted_protections = [(dto.path.lower().replace('\\', '/').rstrip('/'), dto.id) for dto in protections]
        self.path_retention_id = path_retention_id
        self.default_protection_id = default_path_protection_id

        self.sorted_protections: list[tuple[str, int]] = sorted(
            self.sorted_protections,
            key=lambda item: item[0].count('/'),
            reverse=True
        )

    # returns: Retention(retention_id, pathprotection_id) or None if not found
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

class ExternalToInternalRetentionTypeConverter:
    retention_type_dict: dict[str, RetentionTypeDTO]

    def __init__(self, retention_type_dict: dict[str, RetentionTypeDTO]):
        if not retention_type_dict :
            raise ValueError("missing retentention_type_dict for ExternalRetentionTypeConverter")

        valid_values = [e.value.lower() for e in ExternalRetentionTypes if e.value is not None]
        self.retention_type_dict = { key: retentiontype for key, retentiontype in retention_type_dict.items() if key in valid_values }

    def to_internal(self, external_retention: ExternalRetentionTypes) -> RetentionTypeDTO:
        internal_retention:RetentionTypeDTO = None
        if external_retention is not None and external_retention != ExternalRetentionTypes.Unknown:
            internal_retention = self.retention_type_dict.get(external_retention.value.lower(), None)
            if internal_retention is None:
                raise ValueError(f"cannot map external retention type '{external_retention}' to any internal retention type")

        return internal_retention
