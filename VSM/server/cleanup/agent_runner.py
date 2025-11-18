import random
from typing import Callable
from contextlib import contextmanager
from abc import ABC, abstractmethod
from cleanup import agents_internal, agent_on_premise_scan, agent_on_premise_clean


class AgentCallbackHandler(ABC):
    #Abstract base class for handling agent execution callbacks.
    
    @abstractmethod
    def on_agent_postrun(self, agent_info: agents_internal.AgentInfo, task: agents_internal.CleanupTaskDTO | None, 
                         error_message: str | None, success_message: str | None) -> None:
        #Called before an agent runs with agent_info and the task being executed to be executed.
        # error_message: Error message if the agent failed
        # success_message: Success message if the agent succeeded
        pass

class InternalAgentFactory:
    # Factory for managing internal agents with support for dependency injection.   
    
    # By default, agents auto-register themselves. For testing, you can provide
    # a custom list of agents using the context manager or register methods.

    # Example usage in tests:
    #     with InternalAgentFactory.with_agents(test_agents):
    #         run_scheduler_tasks()
    
    # Class-level registry that can be overridden for testing
    _agent_registry: list[agents_internal.AgentTemplate] | None = None
    _default_agents_factory: Callable[[], list[agents_internal.AgentTemplate]] | None = None
    
    @staticmethod
    def _create_default_agents() -> list[agents_internal.AgentTemplate]:
        # Create the default set of production agents.
        return [
            agents_internal.AgentCalendarCreation(),
            agent_on_premise_scan.AgentScanVTSRootFolder(),
            agents_internal.AgentMarkSimulationsPreReview(),
            agents_internal.AgentNotification(),
            agents_internal.AgentNotification(),
            agent_on_premise_clean.AgentCleanVTSRootFolder(),
            agents_internal.AgentUnmarkSimulationsPostReview(),
            agents_internal.AgentFinaliseCleanupCycle(),
        ]
    
    @staticmethod
    def get_internal_agents() -> list[agents_internal.AgentTemplate]:
        # Returns the registered agents if set, otherwise creates default agents.
        # This allows for dependency injection in tests while maintaining default behavior in production.
        if InternalAgentFactory._agent_registry is not None:
            return InternalAgentFactory._agent_registry
        
        if InternalAgentFactory._default_agents_factory is not None:
            return InternalAgentFactory._default_agents_factory()
        
        return InternalAgentFactory._create_default_agents()
    
    @staticmethod
    def register_agents(agents: list[agents_internal.AgentTemplate]) -> None:
        # Register a custom list of agents (for testing) to use instead of defaults
        InternalAgentFactory._agent_registry = agents
    
    @staticmethod
    def register_agents_factory(factory: Callable[[], list[agents_internal.AgentTemplate]]) -> None:
        # Register a factory function for advanced testing. It must return a list of AgentTemplate instances.
        InternalAgentFactory._default_agents_factory = factory
    
    @staticmethod
    def reset_to_defaults() -> None:
        # Reset to default agent configuration (useful for reverting to production setup after a test).
        InternalAgentFactory._agent_registry = None
        InternalAgentFactory._default_agents_factory = None
    
    @staticmethod
    @contextmanager
    def with_agents(agents: list[agents_internal.AgentTemplate]):
        # Context manager for temporarily using custom agents.
        # This is the recommended approach for testing as it ensures proper cleanup.
        
        # Args:
        #     agents: List of agent instances to use temporarily
            
        # Usage:
        #     test_agents = [AgentCalendarCreation(), AgentScanVTSRootFolder()]
        #     with InternalAgentFactory.with_agents(test_agents):
        #         run_scheduler_tasks()
        #         # ... test assertions ...

        old_registry = InternalAgentFactory._agent_registry
        old_factory = InternalAgentFactory._default_agents_factory
        
        try:
            InternalAgentFactory.register_agents(agents)
            yield
        finally:
            InternalAgentFactory._agent_registry = old_registry
            InternalAgentFactory._default_agents_factory = old_factory


    @staticmethod
    def run_internal_agents(
        agents: list[agents_internal.AgentTemplate] | None = None,
        callback_handler: AgentCallbackHandler | None = None,
        run_randomized: bool = False
    ) -> dict[str, any]:
        # Run internal agents.        
        # Args:
        #     agents: Optional list of agents to run. If None, uses registered/default agents.
        #     run_randomized: If True, shuffle agent execution order
        #     callback_handler: Optional handler for agent execution callbacks
        # Returns:
        #     Dictionary with execution results

        agent_list = agents if agents is not None else InternalAgentFactory.get_internal_agents()
        
        if run_randomized:
            random.shuffle(agent_list)
        
        for agent in agent_list:
            agent.run()
            
            # Call postrun callback if handler provided
            if callback_handler is not None:
                callback_handler.on_agent_postrun(agent.agent_info, agent.task, agent.error_message, agent.success_message)
        
        return {"message": f"Internal agents called successfully ({len(agent_list)} agents)"}
