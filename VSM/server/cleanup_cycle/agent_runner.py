from typing import Callable
from contextlib import contextmanager
from cleanup_cycle.internal_agents import AgentTemplate, AgentCalendarCreation, AgentCleanupCycleStart, AgentNotification, AgentCleanupCycleFinishing, AgentCleanupCyclePrepareNext
from cleanup_cycle.on_premise_scan_agent import AgentScanVTSRootFolder
from cleanup_cycle.on_premise_clean_agent import AgentCleanVTSRootFolder

class InternalAgentFactory:
    # Factory for managing internal agents with support for dependency injection.   
    
    # By default, agents auto-register themselves. For testing, you can provide
    # a custom list of agents using the context manager or register methods.

    # Example usage in tests:
    #     with InternalAgentFactory.with_agents(test_agents):
    #         run_scheduler_tasks()
    
    # Class-level registry that can be overridden for testing
    _agent_registry: list[AgentTemplate] | None = None
    _default_agents_factory: Callable[[], list[AgentTemplate]] | None = None
    
    @staticmethod
    def _create_default_agents() -> list[AgentTemplate]:
        # Create the default set of production agents.
        return [
            AgentCalendarCreation(),
            AgentScanVTSRootFolder(),
            AgentCleanupCycleStart(),
            AgentNotification(),
            AgentCleanVTSRootFolder(),
            AgentCleanupCycleFinishing(),
            AgentCleanupCyclePrepareNext(),
        ]
    
    @staticmethod
    def get_internal_agents() -> list[AgentTemplate]:
        # Returns the registered agents if set, otherwise creates default agents.
        # This allows for dependency injection in tests while maintaining default behavior in production.
        if InternalAgentFactory._agent_registry is not None:
            return InternalAgentFactory._agent_registry
        
        if InternalAgentFactory._default_agents_factory is not None:
            return InternalAgentFactory._default_agents_factory()
        
        return InternalAgentFactory._create_default_agents()
    
    @staticmethod
    def register_agents(agents: list[AgentTemplate]) -> None:
        # Register a custom list of agents (for testing) to use instead of defaults
        InternalAgentFactory._agent_registry = agents
    
    @staticmethod
    def register_agents_factory(factory: Callable[[], list[AgentTemplate]]) -> None:
        # Register a factory function for advanced testing. It must return a list of AgentTemplate instances.
        
        InternalAgentFactory._default_agents_factory = factory
    
    @staticmethod
    def reset_to_defaults() -> None:
        # Reset to default agent configuration (useful for reverting to production setup after a test).
        InternalAgentFactory._agent_registry = None
        InternalAgentFactory._default_agents_factory = None
    
    @staticmethod
    @contextmanager
    def with_agents(agents: list[AgentTemplate]):
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
        agents: list[AgentTemplate] | None = None,
        run_randomized: bool = False
    ) -> dict[str, any]:
        # Run internal agents.        
        # Args:
        #     agents: Optional list of agents to run. If None, uses registered/default agents.
        #     run_randomized: If True, shuffle agent execution order            
        # Returns:
        #     Dictionary with execution results

        agent_list = agents if agents is not None else InternalAgentFactory.get_internal_agents()
        
        if run_randomized:
            import random
            random.shuffle(agent_list)
        
        for agent in agent_list:
            agent.run()
        
        return {"message": f"Internal agents called successfully ({len(agent_list)} agents)"}
