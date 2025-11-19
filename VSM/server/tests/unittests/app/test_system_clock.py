"""Unit tests for SystemClock time control."""
import pytest
from datetime import datetime, timedelta, timezone
from app.clock import SystemClock


class TestSystemClock:
    """Test SystemClock time control functionality."""
    
    def setup_method(self):
        """Reset clock to real time before each test."""
        SystemClock.set_real()
    
    def teardown_method(self):
        """Reset clock to real time after each test."""
        SystemClock.set_real()
    
    def test_real_mode_default(self):
        """Test that clock defaults to real mode."""
        assert SystemClock.is_real()
        assert SystemClock.get_mode() == SystemClock.Mode.REAL
    
    def test_real_mode_returns_current_time(self):
        """Test that real mode returns actual current time."""
        before = datetime.now()
        clock_now = SystemClock.now()
        after = datetime.now()
        
        # Clock time should be between before and after
        assert before <= clock_now <= after
    
    def test_fixed_mode(self):
        """Test fixed time mode."""
        fixed_time = datetime(2025, 12, 31, 12, 0, 0)
        SystemClock.set_fixed(fixed_time)
        
        assert not SystemClock.is_real()
        assert SystemClock.get_mode() == SystemClock.Mode.FIXED
        assert SystemClock.now() == fixed_time
        
        # Should return same time even after waiting
        import time
        time.sleep(0.1)
        assert SystemClock.now() == fixed_time
    
    def test_offset_mode_seconds(self):
        """Test time offset mode with seconds."""
        offset = 3600  # 1 hour
        SystemClock.set_offset_seconds(offset)
        
        assert SystemClock.get_mode() == SystemClock.Mode.OFFSET
        
        before = datetime.now() + timedelta(seconds=offset)
        clock_now = SystemClock.now()
        after = datetime.now() + timedelta(seconds=offset)
        
        # Clock should be roughly offset seconds ahead
        assert before <= clock_now <= after
    
    def test_offset_mode_days(self):
        """Test time offset mode with days."""
        offset_days = 7  # 1 week
        SystemClock.set_offset_days(offset_days)
        
        assert SystemClock.get_mode() == SystemClock.Mode.OFFSET
        
        before = datetime.now() + timedelta(days=offset_days)
        clock_now = SystemClock.now()
        after = datetime.now() + timedelta(days=offset_days)
        
        # Clock should be roughly 7 days ahead
        assert before <= clock_now <= after
    
    def test_negative_offset(self):
        """Test negative offset (time in the past)."""
        offset = -86400  # -1 day
        SystemClock.set_offset_seconds(offset)
        
        before = datetime.now() + timedelta(seconds=offset)
        clock_now = SystemClock.now()
        after = datetime.now() + timedelta(seconds=offset)
        
        # Clock should be roughly 1 day behind
        assert before <= clock_now <= after
    
    def test_today_method(self):
        """Test today() returns correct date."""
        fixed_time = datetime(2025, 12, 25, 15, 30, 0)
        SystemClock.set_fixed(fixed_time)
        
        assert SystemClock.today() == fixed_time.date()
    
    def test_utcnow_method(self):
        """Test utcnow() method."""
        fixed_time = datetime(2025, 12, 25, 12, 0, 0)
        SystemClock.set_fixed(fixed_time)
        
        assert SystemClock.utcnow() == fixed_time
    
    def test_reset_to_real(self):
        """Test resetting clock back to real time."""
        # Set to fixed
        fixed_time = datetime(2025, 12, 31, 12, 0, 0)
        SystemClock.set_fixed(fixed_time)
        assert SystemClock.now() == fixed_time
        
        # Reset to real
        SystemClock.set_real()
        assert SystemClock.is_real()
        
        # Should now return actual current time
        before = datetime.now()
        clock_now = SystemClock.now()
        after = datetime.now()
        assert before <= clock_now <= after
    
    def test_mode_switching(self):
        """Test switching between different modes."""
        # Start with offset
        SystemClock.set_offset_days(7)
        assert SystemClock.get_mode() == SystemClock.Mode.OFFSET
        
        # Switch to fixed
        fixed_time = datetime(2026, 1, 1, 0, 0, 0)
        SystemClock.set_fixed(fixed_time)
        assert SystemClock.get_mode() == SystemClock.Mode.FIXED
        assert SystemClock.now() == fixed_time
        
        # Switch back to real
        SystemClock.set_real()
        assert SystemClock.is_real()
    
    def test_timezone_aware_real_mode(self):
        """Test that timezone parameter works in real mode."""
        before = datetime.now(timezone.utc)
        clock_now = SystemClock.now(timezone.utc)
        after = datetime.now(timezone.utc)
        
        # Clock time should be between before and after
        assert before <= clock_now <= after
        # Should be timezone-aware
        assert clock_now.tzinfo == timezone.utc
    
    def test_timezone_aware_fixed_mode(self):
        """Test timezone parameter with fixed time."""
        fixed_time = datetime(2025, 12, 31, 12, 0, 0)
        SystemClock.set_fixed(fixed_time)
        
        # Without timezone - returns naive
        naive_time = SystemClock.now()
        assert naive_time.tzinfo is None
        assert naive_time == fixed_time
        
        # With timezone - returns timezone-aware
        aware_time = SystemClock.now(timezone.utc)
        assert aware_time.tzinfo == timezone.utc
        assert aware_time.replace(tzinfo=None) == fixed_time
    
    def test_timezone_aware_offset_mode(self):
        """Test timezone parameter with offset mode."""
        offset = 3600  # 1 hour
        SystemClock.set_offset_seconds(offset)
        
        before = datetime.now(timezone.utc) + timedelta(seconds=offset)
        clock_now = SystemClock.now(timezone.utc)
        after = datetime.now(timezone.utc) + timedelta(seconds=offset)
        
        # Clock should be roughly offset seconds ahead
        assert before <= clock_now <= after
        # Should be timezone-aware
        assert clock_now.tzinfo == timezone.utc
