"""
Common utilities for Collibra Bulk Exporter

This module provides common utility functions used across the application.
"""

import os
import sys
import time
import codecs
import logging
from functools import wraps

def setup_logging():
    """
    Configure logging with both file and console handlers with proper formatting.
    
    Sets up multiple log handlers:
    - Console handler (stdout)
    - Timestamped file handler
    - Latest log file handler
    - Debug log file handler
    """
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'logs/app_{time.strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
            logging.FileHandler('logs/latest.log', encoding='utf-8', mode='w')
        ]
    )
    
    # Add debug file handler
    debug_handler = logging.FileHandler('logs/debug.log', encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(logging.Formatter(log_format, date_format))
    logging.getLogger().addHandler(debug_handler)
    
    # Set console encoding to UTF-8 for Windows
    if sys.platform == 'win32':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')
        os.system('chcp 65001')
        logging.debug("Windows console encoding set to UTF-8")

class PerformanceLogger:
    """
    Context manager for logging execution time of code blocks.
    
    Example:
        with PerformanceLogger("operation_name"):
            # code to measure
    """
    def __init__(self, operation_name):
        """
        Initialize with the name of the operation being measured.
        
        Args:
            operation_name (str): Name of the operation to log
        """
        self.operation_name = operation_name
        self.start_time = None

    def __enter__(self):
        """Start the timer when entering the context."""
        self.start_time = time.time()
        logging.debug(f"Starting {self.operation_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Log the duration when exiting the context."""
        duration = time.time() - self.start_time
        if exc_type:
            logging.error(f"{self.operation_name} failed after {duration:.2f} seconds")
        else:
            logging.debug(f"{self.operation_name} completed in {duration:.2f} seconds")

def performance_logger(func):
    """
    Decorator to log the execution time of a function.
    
    Args:
        func: The function to be decorated
        
    Returns:
        The wrapped function with performance logging
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        operation_name = func.__name__
        with PerformanceLogger(operation_name):
            return func(*args, **kwargs)
    return wrapper

def is_empty(value):
    """
    Check if a value is considered empty.
    
    Args:
        value: The value to check
        
    Returns:
        bool: True if the value is empty, False otherwise
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ''
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False

def safe_convert_to_str(value):
    """
    Handle special characters and encodings when converting to string.
    
    Args:
        value: The value to convert
        
    Returns:
        str or None: The converted string or None if conversion fails
    """
    if value is None:
        return None
        
    try:
        if isinstance(value, (list, tuple)):
            return ', '.join(str(v).encode('ascii', 'ignore').decode('ascii') 
                            for v in value if v is not None)
        return str(value).encode('ascii', 'ignore').decode('ascii')
    except Exception as e:
        logging.error(f"Error converting value to string: {e}")
        return None

def sanitize_identifier(name):
    """
    Sanitize table/column names for database compatibility.
    
    Args:
        name (str): The name to sanitize
        
    Returns:
        str: The sanitized name
    """
    if name is None:
        return 'unnamed'
    
    # Replace common problematic characters
    sanitized = name.lower()
    sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in sanitized)
    
    # Ensure it starts with a letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    
    # Remove consecutive underscores and trim length
    while '__' in sanitized:
        sanitized = sanitized.replace('__', '_')
    
    # Trim to PostgreSQL's maximum identifier length (63 characters)
    sanitized = sanitized[:63]
    
    # Remove trailing underscores
    sanitized = sanitized.rstrip('_')
    
    return sanitized
