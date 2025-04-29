"""Database connection and session management for SQLAlchemy."""

import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession  # Rename to avoid confusion
from contextlib import contextmanager
from typing import Generator

# Import config helper to get connection string
from ..utils import config_helpers

# Global engine and session factory (initialized once)
_engine = None
_SessionFactory = None


def _initialize_db():
    """Initializes the SQLAlchemy engine and session factory."""
    global _engine, _SessionFactory
    if _SessionFactory:
        return  # Already initialized

    try:
        connection_string = config_helpers.get_required_env_var("DB_CONNECTION_STRING")
        # echo=True is useful for debugging SQL locally, disable in production
        _engine = create_engine(connection_string, pool_recycle=3600, echo=False)
        _SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
        logging.info("Database engine and session factory initialized.")
    except Exception as e:
        logging.exception("Failed to initialize database connection: %s", e)
        # Depending on requirements, you might want to prevent app startup here
        # or allow it to run without DB access initially.
        raise


def get_db_session() -> SQLAlchemySession:
    """Provides a new database session."""
    if not _SessionFactory:
        _initialize_db()  # Attempt initialization if not done yet
    if not _SessionFactory:  # Check again after attempt
        raise RuntimeError("Database session factory is not initialized.")

    # noinspection PyCallingNonCallable
    session = _SessionFactory()
    return session


@contextmanager
def session_scope() -> Generator[SQLAlchemySession, None, None]:
    """Provide a transactional scope around a series of operations."""
    session = get_db_session()
    logging.debug(f"DB Session {id(session)} obtained.")
    try:
        yield session
        # Commit is typically handled by the calling function
        # session.commit()
    except Exception:
        logging.exception("Exception occurred within session scope, rolling back.")
        session.rollback()
        raise
    finally:
        logging.debug(f"DB Session {id(session)} closed.")
        session.close()

# Call initialization once when the module is loaded in the worker.
# Handles potential issues if multiple function invocations try to init simultaneously.
# _initialize_db() # Or lazily initialize on first call to get_db_session() (current implementation)
