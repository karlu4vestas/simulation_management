import asyncio
from datetime import date, timedelta 
from cleanup_cycle.cleanup_dtos import ActionType, AgentInfo, CleanupTaskDTO, TaskStatus 
from cleanup_cycle.cleanup_db_actions import cleanup_cycle_start, cleanup_cycle_finishing, cleanup_cycle_prepare_next_cycle
from cleanup_cycle.cleanup_scheduler import CleanupScheduler, AgentInterfaceMethods

# ----------------- AgentTemplate -----------------
from abc import ABC, abstractmethod

class AgentTemplate(ABC):
    agent_info: AgentInfo
    task: CleanupTaskDTO | None
    error_message: str | None
    success_message: str | None

    def __init__(self, agent_id: str, action_types: list[str], supported_storage_ids: list[str]|None = None):
        self.agent_info = AgentInfo(agent_id=agent_id, action_types=action_types, supported_storage_ids=supported_storage_ids)

    def __repr__(self):
        return f"AgentTemplate(agent_info={self.agent_info})"

    def run(self):
        self.reserve_task()
        if self.task is not None:
            asyncio.run(self.execute_task())
        self.complete_task()

    def reserve_task(self):
        self.task = AgentInterfaceMethods.reserve_task(self.agent_info)

    def complete_task(self ):
        if self.task is not None:
            if self.error_message is not None:
                AgentInterfaceMethods.task_completion(self.task.id, TaskStatus.FAILED.value, self.error_message)
            else:
                AgentInterfaceMethods.task_completion(self.task.id, TaskStatus.COMPLETED.value, "Task executed successfully")

    @abstractmethod
    async def execute_task(self):
        # Subclasses must implement this method to execute their specific task logic.
        pass


# ----------------- internal agents implementations -----------------
class AgentCalendarCreation(AgentTemplate):

    def __init__(self):
        super().__init__("AgentCalendarCreation", [ActionType.CREATE_CLEANUP_CALENDAR.value])

    def execute_task(self):
        msg: str = CleanupScheduler.create_calendars_for_cleanup_configuration_ready_to_start()
        self.success_message = f"CalendarCreation done with: {msg}"

class AgentCleanupCycleStart(AgentTemplate):

    def __init__(self):
        super().__init__("AgentCleanupCycleStart", [ActionType.START_RETENTION_REVIEW.value])

    def execute_task(self):
        cleanup_cycle_start(self.task.rootfolder_id)
        self.success_message = f"Cleanup cycle started for rootfolder {self.task.rootfolder_id}"

class AgentCleanupCycleFinishing(AgentTemplate):

    def __init__(self):
        super().__init__("AgentCleanupCycleFinishing", [ActionType.FINISH_CLEANUP_CYCLE.value])

    def execute_task(self):
        cleanup_cycle_finishing(self.task.rootfolder_id)
        self.success_message = f"Cleanup cycle finishing for rootfolder {self.task.rootfolder_id}"

class AgentCleanupCyclePrepareNext(AgentTemplate):
    def __init__(self):
        super().__init__("AgentCleanupCyclePrepareNext", [ActionType.PREPARE_NEXT_CLEANUP_CYCLE.value])

    def execute_task(self):
        cleanup_cycle_prepare_next_cycle(self.task.rootfolder_id)
        self.success_message = f"Next cleanup cycle prepared for rootfolder {self.task.rootfolder_id}"


class AgentNotification(AgentTemplate):
    def __init__(self):
        super().__init__("AgentNotification", [ActionType.SEND_INITIAL_NOTIFICATION.value, ActionType.SEND_FINAL_NOTIFICATION.value])
    
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
        self.error_message = f"sending email + to {receivers} receivers.\nEmail content: {message} "    
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
        from db.database import Database
        from datamodel.dtos import RootFolderDTO, CleanupConfigurationDTO, CleanupProgress
        from sqlmodel import Session, func, select
        with Session(Database.get_engine()) as session:
            rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == self.task.rootfolder_id)).first()
            config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session) if rootfolder is not None else None
            if rootfolder is None or config is None:
                self.error_message = f"RootFolder with ID {self.task.rootfolder_id} not found."
            else:
                enddate_for_cleanup_cycle: date = config.cleanup_start_date + timedelta(days=config.cleanupfrequency-1)
                initial_message: str = f"The review has started. Use it to review and adjust the retention of your simulation in particular those marked for cleanup \n" + \
                                    f"You have configure the cleanup rutine as follow " + \
                                    f"Duration of the review periode is {config.cleanupfrequency_days} days; ending on {enddate_for_cleanup_cycle}." + \
                                    f"Simulations will be marked for cleanup {config.cycletime} days from last modification date unless otherwise specified by retention settings."
                final_message: str   = f"The retention review is about to end in {config.cleanupfrequency-self.task.task_offset-1} days."

                message: str = initial_message if self.task.action_type == ActionType.SEND_INITIAL_NOTIFICATION.value else final_message

                receivers: list[str] = [] if rootfolder.owner is None else [rootfolder.owner+f"@vestas.com"]
                if rootfolder.approvers is not None:
                    for approver in rootfolder.approvers:
                        receivers.append(approver+f"@vestas.com")
                if len(receivers) == 0:
                    self.error_message = f"No receivers found for RootFolder with ID {self.task.rootfolder_id}."                
                else:
                    self.send_notification(message, receivers)


from cleanup_cycle.on_premise_scan_agent import AgentScanVTSRootFolder
class InternalAgentFactory:
    @staticmethod
    def get_internal_agents() -> list[AgentTemplate]:
        return [
            AgentCalendarCreation(),
            AgentScanVTSRootFolder(),
            AgentCleanupCycleStart(),
            AgentNotification(),
            AgentCleanupCycleFinishing(),
            AgentCleanupCyclePrepareNext(),
    ]

    @staticmethod
    def run_internal_agents() -> dict[str, any]:
        agents: list[AgentTemplate] = InternalAgentFactory.get_internal_agents()
        for agent in agents:
            agent.run()
        return {"message": "Internal agents called successfully"}
