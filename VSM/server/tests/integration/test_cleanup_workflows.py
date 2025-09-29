import pytest
from .base_integration_test import BaseIntegrationTest


@pytest.mark.integration
@pytest.mark.cleanup_workflow
@pytest.mark.slow
class TestCleanupWorkflows(BaseIntegrationTest):
    """Integration tests for complete cleanup workflows"""
    
    def test_full_cleanup_lifecycle(self, integration_session, cleanup_scenario_data):
        """Test complete cleanup workflow from start to finish"""
        session = integration_session
        
        # Step 1: Insert simulations
        simulations = self.setup_simulations(session, cleanup_scenario_data["simulations"])
        assert len(simulations) == 2
        
        # Step 2: Start cleanup round
        cleanup_round = self.start_cleanup_round(session, duration_weeks=4)
        assert cleanup_round.status == "active"
        
        # Step 3: Update retention during cleanup
        retention_changes = {"retention_id": 1, "new_days": 60}
        self.update_retention_during_cleanup(session, retention_changes)
        
        # Step 4: Update simulations during cleanup
        sim_updates = {"sim_old_inactive": {"status": "archived"}}
        self.update_simulations_during_cleanup(session, sim_updates)
        
        # Step 5: Execute cleanup
        results = self.execute_cleanup(session, cleanup_round.id)
        
        # Step 6: Verify final state
        expected = {"cleaned_count": 1, "retained_count": 1}
        self.verify_cleanup_results(session, expected)
        
        assert results["cleaned"] >= expected["cleaned_count"]
    
    def test_retention_policy_changes_during_cleanup(self, integration_session, cleanup_scenario_data):
        """Test how retention policy changes affect ongoing cleanup"""
        session = integration_session
        
        # Setup initial state
        simulations = self.setup_simulations(session, cleanup_scenario_data["simulations"])
        cleanup_round = self.start_cleanup_round(session)
        
        # Change retention policy mid-cleanup
        original_retention = cleanup_scenario_data["retention_policies"][0]
        self.update_retention_during_cleanup(session, {
            "retention_id": original_retention["id"],
            "new_days": original_retention["days"] * 2  # Double retention
        })
        
        # Execute and verify policy change took effect
        results = self.execute_cleanup(session, cleanup_round.id)
        
        # Should retain more items due to extended retention
        expected = {"cleaned_count": 0, "retained_count": 2}
        self.verify_cleanup_results(session, expected)

    def test_simulation_updates_during_cleanup(self, integration_session, cleanup_scenario_data):
        """Test simulation modifications during active cleanup round"""
        session = integration_session
        
        simulations = self.setup_simulations(session, cleanup_scenario_data["simulations"])
        cleanup_round = self.start_cleanup_round(session)
        
        # Update simulation status during cleanup
        self.update_simulations_during_cleanup(session, {
            "sim_old_inactive": {"status": "active"}  # Reactivate old sim
        })
        
        results = self.execute_cleanup(session, cleanup_round.id)
        
        # Reactivated simulation should be retained
        expected = {"cleaned_count": 0, "retained_count": 2}
        self.verify_cleanup_results(session, expected)
