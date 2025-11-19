from datetime import date, timedelta
from sqlmodel import Session, select
from fastapi import HTTPException
from db.database import Database
from datamodel import dtos, retentions
from cleanup import agent_db_interface
from cleanup.scheduler_dtos import ActionType, AgentInfo, CleanupTaskDTO, TaskStatus 
from cleanup.scheduler import CleanupScheduler
from cleanup.agent_task_manager import AgentTaskManager
from db import db_api

# ----------------- AgentTemplate -----------------
from abc import ABC, abstractmethod

class AgentTemplate(ABC):
    agent_info: AgentInfo
    task: CleanupTaskDTO | None
    error_message: str | None
    success_message: str | None

    def __init__(self, agent_id: str, action_types: list[str], supported_storage_ids: list[str]|None = None):
        self.agent_info = AgentInfo(agent_id=agent_id, action_types=action_types, supported_storage_ids=supported_storage_ids)
        self.task = None
        self.error_message = None
        self.success_message = None

    def __repr__(self):
        return f"AgentTemplate(agent_info={self.agent_info})"

    def run(self):
        self.reserve_task()
        if self.task is not None:
            self.execute_task()
        self.complete_task()

    def reserve_task(self):
        self.task = AgentTaskManager.reserve_task(self.agent_info)

    def complete_task(self ):
        if self.task is not None:
            if self.error_message is not None:
                AgentTaskManager.task_completion(self.task.id, TaskStatus.FAILED.value, self.error_message)
            else:
                AgentTaskManager.task_completion(self.task.id, TaskStatus.COMPLETED.value, "Task executed successfully")

    @abstractmethod
    def execute_task(self):
        # Subclasses must implement this method to execute their specific task logic.
        pass


# ----------------- internal agents implementations -----------------
class AgentCalendarCreation(AgentTemplate):
    # this ia a fake agent because it does not require a task and will always be run when called
    # In fact the agent calls the scheduler to create calendars and tasks for rootfolder that are ready to start cleanup cycles
    def __init__(self):
        super().__init__("AgentCalendarCreation", [])

    #overide this because no task reservation is needed
    def run(self):
        self.execute_task()

    def execute_task(self):
        msg: str = CleanupScheduler.create_calendars_for_cleanup_configuration_ready_to_start()
        CleanupScheduler.update_calendars_and_tasks() # prepare so the next task can be activated right away
        self.success_message = f"CalendarCreation done with: {msg}"

class AgentMarkSimulationsPreReview(AgentTemplate):

    def __init__(self):
        super().__init__("AgentCleanupCycleStart", [ActionType.MARK_SIMULATIONS_FOR_REVIEW.value])

    def execute_task(self):
        AgentMarkSimulationsPreReview.mark_simulations(self.task.rootfolder_id)
        self.success_message = f"Cleanup cycle started for rootfolder {self.task.rootfolder_id}"

    @staticmethod
    def mark_simulations(rootfolder_id: int) -> dict[str, str]:
        # recalculating retentions for all leaf folders in the rootfolder
        db_api.apply_pathprotections(rootfolder_id)  # ThIS should noT be necessary but just to be sure that faulty transactions did not miss any pathprotections    
        len_folders:int = 0
        with Session(Database.get_engine()) as session:
            rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
            cleanup_config:dtos.CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session) if rootfolder else None

            if not rootfolder or not cleanup_config:
                raise HTTPException(status_code=404, detail="rootfolder or cleanup_config not found")

            retention_calculator: retentions.RetentionCalculator = retentions.RetentionCalculator(rootfolder_id, cleanup_config.id, session)

            # recalculate numeric retentions for all simulations.
            # @TODO This can possibly be optimise by limiting the selection to folders with a numeric retentiontypes
            nodetype_leaf_id: int = db_api.read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[dtos.FolderTypeEnum.SIMULATION].id
            folders = session.exec( select(dtos.FolderNodeDTO).where( (dtos.FolderNodeDTO.rootfolder_id == rootfolder_id) & \
                                                                      (dtos.FolderNodeDTO.nodetype_id == nodetype_leaf_id) ) ).all()
            
            # update retention: This also mark simulations for cleanup if they are ready
            for folder in folders:
                folder.set_retention(retention_calculator.adjust_from_cleanup_configuration_and_modified_date(folder.get_retention(), folder.modified_date))
                session.add(folder)
            
            session.commit()
            len_folders = len(folders)

        return {"message": f"new cleanup cycle started for : {rootfolder_id}. updated retention of {len_folders} folders" }


class AgentUnmarkSimulationsPostReview(AgentTemplate):
    #post pone the retention of marked simulations if any remaining in that state after the review and cleaning phase
    def __init__(self):
        super().__init__("AgentUnmarkSimulationsPostReview", [ActionType.UNMARK_SIMULATIONS_AFTER_REVIEW.value])

    def execute_task(self):
        AgentUnmarkSimulationsPostReview.unmark_simulations_post_review(self.task.rootfolder_id)
        
        self.success_message = f"Cleanup cycle finishing for rootfolder {self.task.rootfolder_id}"

    @staticmethod
    def unmark_simulations_post_review(rootfolder_id: int) -> dict[str, str]:
        with Session(Database.get_engine()) as session:
            rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()

            marked_simulations:list[dtos.FolderNodeDTO] = db_api.read_folders_marked_for_cleanup(rootfolder_id)
            if len(marked_simulations) > 0:
                # change the retention to the next retention after marked
                retention_calculator: retentions.RetentionCalculator = retentions.RetentionCalculator(rootfolder_id, rootfolder.cleanup_config_id, session)
                after_marked_retention_id:int                        = retention_calculator.get_retention_id_after_marked()
                for folder in marked_simulations:
                    folder.retention_id = after_marked_retention_id # thois that we not clean will marked for the next cleanup round
                    session.add(folder)

            session.commit()

        return {"message": f"Finished cleanup cycle for rootfolder {rootfolder_id}"}

class AgentFinaliseCleanupCycle(AgentTemplate):
    def __init__(self):
        super().__init__("AgentFinaliseCleanupCycle", [ActionType.FINALISE_CLEANUP_CYCLE.value])

    def execute_task(self):
        # nothing to do for now. WE just need this to go to the next state
        # cleanup_db_actions.cleanup_cycle_prepare_next_cycle(self.task.rootfolder_id)
        self.success_message = f"Current cleanup cycle was finalised for rootfolder {self.task.rootfolder_id}"


class AgentNotification(AgentTemplate):
    def __init__(self):
        super().__init__("AgentNotification", [ActionType.SEND_INITIAL_NOTIFICATION.value, ActionType.SEND_FINAL_NOTIFICATION.value])
    
    def run(self):
        self.reserve_task()
        if self.task is not None:
            self.execute_task()
            #asyncio.run(self.execute_task())
        self.complete_task()

    
    def send_notification(self, message: str, receivers: list[str]) -> None:
        # Send email notification to the specified receivers.
        
        # Args:
        #     message: The notification message to send
        #     receivers: List of email addresses to send the notification to


        # Note: You may need to:

        # Update the smtp_server and smtp_port values to match your actual email server
        # Add authentication credentials if your SMTP server requires them (uncomment the starttls() and login() lines)
        # Consider moving email configuration to environment variables or a config file
        # Add the email configuration to your application settings for better maintainability
        # The method will set self.error_message if sending fails, which will cause the task to be marked as FAILED when complete_task() is called.            
        
        #self.error_message = f"sending email + to {receivers} receivers.\nEmail content: {message} "    
        return 
    

        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        try:
            # Email configuration - these should ideally come from environment variables or config
            #smtp_server = "smtp.vestas.com"  # Update with actual SMTP server
            #smtp_port = 587  # or 465 for SSL
            #sender_email = "vsm-notifications@vestas.com"  # Update with actual sender

            smtp_server = "smtp.vestas.com"  # Update with actual SMTP server
            smtp_port = 587  # or 465 for SSL
            sender_email = "vsm-notifications@vestas.com"  # Update with actual sender
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = ", ".join(receivers)
            msg['Subject'] = "VSM Cleanup Notification"
            
            # Add message body
            msg.attach(MIMEText(message, 'plain'))
            
            # Send email
            # Note: This is a basic implementation. In production, you might want to:
            # 1. Use authentication if required
            # 2. Handle SSL/TLS properly
            # 3. Add retry logic
            # 4. Log the email sending activity
            
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                # Uncomment and configure if authentication is needed:
                # server.starttls()
                # server.login(username, password)
                
                server.send_message(msg)
            
            self.success_message = f"Notification sent successfully to {len(receivers)} recipient(s)"
            
        except Exception as e:
            self.error_message = f"Failed to send notification: {str(e)}"

    def execute_task(self):
        from sqlmodel import Session, func, select
        from fastapi import Query, HTTPException
        from db.database import Database
        from datamodel import dtos
        with Session(Database.get_engine()) as session:
            rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == self.task.rootfolder_id)).first()
            config: dtos.CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session) if rootfolder is not None else None
            if rootfolder is None or config is None:
                self.error_message = f"RootFolder with ID {self.task.rootfolder_id} not found."
            else:
                enddate_for_cleanup_cycle: date = config.start_date + timedelta(days=config.frequency-1)
                initial_message: str = f"The review has started. Use it to review and adjust the retention of your simulation in particular those marked for cleanup \n" + \
                                    f"You have configure the cleanup rutine as follow " + \
                                    f"Duration of the review periode is {config.frequency} days; ending on {enddate_for_cleanup_cycle}." + \
                                    f"Simulations will be marked for cleanup {config.leadtime} days from last modification date unless otherwise specified by retention settings."
                final_message: str   = f"The retention review is about to end in {config.frequency-self.task.task_offset-1} days."

                message: str = initial_message if self.task.action_type == ActionType.SEND_INITIAL_NOTIFICATION.value else final_message

                receivers: list[str] = [] if rootfolder.owner is None else [rootfolder.owner+f"@vestas.com"]
                approvers: list[str] = rootfolder.approvers.split(",") if rootfolder.approvers is not None else []
                if approvers:
                    for approver in approvers:
                        receivers.append(approver+f"@vestas.com")
                if len(receivers) == 0:
                    self.error_message = f"No receivers found for RootFolder with ID {self.task.rootfolder_id}."                
                else:
                    self.send_notification(message, receivers)
                    self.success_message = f"Notification task {self.task.action_type} executed for rootfolder {self.task.rootfolder_id}"
