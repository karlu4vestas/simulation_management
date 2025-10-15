from bisect import bisect_left
from typing import Optional
from datetime import date, timedelta
from datamodel.dtos import ExternalRetentionTypes, Retention 
from datamodel.dtos import CleanupConfiguration, RetentionTypeDTO, PathProtectionDTO

#ensure consistency of retentions
#  
class RetentionCalculator:
    def __init__(self, retention_type_dict: dict[str, RetentionTypeDTO], cleanup_config: CleanupConfiguration):
        if not retention_type_dict or not cleanup_config.cleanup_round_start_date or not cleanup_config.cycletime or cleanup_config.cycletime <= 0:
            raise ValueError("cleanup_round_start_date, at least one numeric retention type and cycletime must be set for RetentionCalculator to work")

        self.retention_type_str_dict     = retention_type_dict
        self.retention_type_id_dict      = {retention.id: retention for retention in self.retention_type_str_dict.values()}
        self.cleanup_config              = cleanup_config
        self.cleanup_round_start_date    = cleanup_config.cleanup_round_start_date
        self.cycletimedelta              = timedelta(days=cleanup_config.cycletime)
        self.path_retention_id           = self.retention_type_str_dict["path"].id if self.retention_type_str_dict.get("path", None) is not None else 0  
        self.marked_retention_id         = self.retention_type_str_dict["marked"].id if self.retention_type_str_dict.get("marked", None) is not None else 0  
        self.is_clean_round_active       = cleanup_config.is_clean_round_active

        # create numeric retention values. notice that we exclude the "marked" retentiontype if the cleanup round is inactive
        numeric_retention_types          = {key:retention for key,retention in retention_type_dict.items() if retention.days_to_cleanup is not None }
        if not self.is_clean_round_active:
            numeric_retention_types      = {key:retention for key,retention in numeric_retention_types.items() if retention.id != self.marked_retention_id}
            
        self.numeric_retention_id_dict   = {retention.id: retention for retention in numeric_retention_types.values() }
        self.numeric_retention_ids       = [retention.id for retention in numeric_retention_types.values()]
        self.numeric_retention_durations = [retention.days_to_cleanup for retention in numeric_retention_types.values()]

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
    def adjust_from_cleanup_configuration_and_modified_date(self, retention:Retention, modified_date:date=None) -> Retention:

        retentiontype:RetentionTypeDTO = self.retention_type_id_dict.get(retention.retention_id, None)
        if retentiontype is not None and (retentiontype.id == self.path_retention_id or retentiontype.is_endstage): 
            # path retention has priority so we do not change it
            retention.expiration_date = None
        else:
            if modified_date is not None:
                retention.expiration_date = modified_date + self.cycletimedelta if retention.expiration_date is None else max(retention.expiration_date, modified_date + self.cycletimedelta)

            if retention.expiration_date is None:
                raise ValueError("retention.expiration_date is None in RetentionCalculator adjust_to_cleanup_round")
            else:
                days_to_expiration = (retention.expiration_date - self.cleanup_round_start_date).days
                # find first index where retention_duration[idx] >= days_until_expiration
                idx = bisect_left(self.numeric_retention_durations, days_to_expiration)

                # if days_until_expiration is greater than every threshold, return last index
                retention.retention_id = self.numeric_retention_ids[idx] if idx < len(self.numeric_retention_durations) else self.numeric_retention_ids[len(self.numeric_retention_durations) - 1]
        return retention

    # adjust numeric retention_type to the new cleanup_configuration
    #   - This is what you what when starting a new cleanup round
    #
    # if non numeric retention then set expiration_date to None
    # if numeric retention then update numeric retention_id to the expiration_date and the cleanup_configuration
    def adjust_retentions_from_cleanup_configuration(self, retention:Retention) -> Retention:
        return self.adjust_from_cleanup_configuration_and_modified_date(retention)

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
