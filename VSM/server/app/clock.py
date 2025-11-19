# System clock abstraction for time control in tests and production.
# Provides a centralized way to get current time that can be overridden for testing.

from datetime import datetime, timedelta, date, timezone
from typing import Optional


class SystemClock:
    """
    Centralized clock for application-wide time access.
    
    Usage:
        Production (default):
            SystemClock.now()  # returns datetime.now()
            SystemClock.now(timezone.utc)  # returns datetime.now(timezone.utc)
        
        Testing - fixed time:
            SystemClock.set_fixed(datetime(2025, 12, 31, 12, 0, 0))
            SystemClock.now()  # always returns 2025-12-31 12:00:00
            SystemClock.now(timezone.utc)  # returns as UTC-aware
        
        Testing - time offset:
            SystemClock.set_offset_seconds(86400)  # 1 day ahead
            SystemClock.now()  # returns datetime.now() + 1 day
            SystemClock.now(timezone.utc)  # returns datetime.now(timezone.utc) + 1 day
        
        Reset to real time:
            SystemClock.set_real()
    """
    
    class Mode:
        REAL = "real"
        FIXED = "fixed"
        OFFSET = "offset"
    
    _mode: str = Mode.REAL
    _fixed_now: Optional[datetime] = None
    _offset: timedelta = timedelta(0)
    
    @classmethod
    def now(cls, tz=None) -> datetime:
        """
        Get current datetime.
        
        Args:
            tz: Optional timezone (e.g., timezone.utc). If provided, returns timezone-aware datetime.
                If None, returns naive local time (default behavior).
        
        Returns:
            datetime: Current time, naive or timezone-aware based on tz parameter
        """
        if cls._mode == cls.Mode.FIXED:
            if tz is not None and cls._fixed_now.tzinfo is None:
                # If requesting timezone-aware but fixed time is naive, assume it's UTC
                return cls._fixed_now.replace(tzinfo=tz)
            return cls._fixed_now
        
        base = datetime.now(tz)
        if cls._mode == cls.Mode.OFFSET:
            return base + cls._offset
        
        return base
    
    @classmethod
    def utcnow(cls) -> datetime:
        """Get current UTC datetime (naive)."""
        if cls._mode == cls.Mode.FIXED:
            # Treat fixed_now as naive UTC
            return cls._fixed_now
        
        base = datetime.utcnow()
        if cls._mode == cls.Mode.OFFSET:
            return base + cls._offset
        
        return base
    
    @classmethod
    def today(cls) -> date:
        """Get current date."""
        return cls.now().date()
    
    # Configuration methods
    
    @classmethod
    def set_fixed(cls, dt: datetime) -> None:
        """
        Set clock to always return a fixed datetime.
        
        Args:
            dt: The fixed datetime to return from now()
        """
        cls._mode = cls.Mode.FIXED
        cls._fixed_now = dt
        cls._offset = timedelta(0)
    
    @classmethod
    def set_offset_seconds(cls, seconds: int) -> None:
        """
        Set clock to return real time plus/minus an offset.
        
        Args:
            seconds: Number of seconds to offset (positive for future, negative for past)
        """
        cls._mode = cls.Mode.OFFSET
        cls._offset = timedelta(seconds=seconds)
        cls._fixed_now = None
    
    @classmethod
    def set_offset_days(cls, days: int) -> None:
        """
        Set clock to return real time plus/minus days.
        
        Args:
            days: Number of days to offset (positive for future, negative for past)
        """
        cls.set_offset_seconds(days * 86400)

    @classmethod
    def get_offset_days(cls) -> int:
        # Get current offset in days.
        return cls._offset.days

    @classmethod
    def set_real(cls) -> None:
        """Reset clock to real system time (production mode)."""
        cls._mode = cls.Mode.REAL
        cls._fixed_now = None
        cls._offset = timedelta(0)
    
    @classmethod
    def get_mode(cls) -> str:
        """Get current clock mode for diagnostics."""
        return cls._mode
    
    @classmethod
    def is_real(cls) -> bool:
        """Check if clock is in real time mode."""
        return cls._mode == cls.Mode.REAL
