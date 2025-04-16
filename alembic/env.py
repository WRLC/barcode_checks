"""Alembic environment script for database migrations."""

# alembic/env.py
import os
import sys
from logging.config import fileConfig
import logging  # Make sure logging is imported

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Import Base from your models ---
from shared_code.database.models import Base  # <<<< IMPORT YOUR BASE

# ------------------------------------

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- Set target metadata ---
target_metadata = Base.metadata  # <<<< POINT TO YOUR BASE METADATA


# ---------------------------

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

def get_url():
    """Return database URL from environment variable."""
    url = os.getenv("DB_CONNECTION_STRING")
    # Add logging here to confirm if the env var is found by env.py
    if url:
        logging.info("Alembic env.py: Found DB_CONNECTION_STRING environment variable.")
    else:
        logging.warning("Alembic env.py: DB_CONNECTION_STRING environment variable not found.")
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_url()
    if not url:
        # Fallback to ini file if needed for offline, though usually requires env var too
        url = config.get_main_option("sqlalchemy.url")
        if not url:
            raise ValueError(
                "Offline mode requires DB_CONNECTION_STRING env var or sqlalchemy.url in alembic.ini"
            )

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Get the config section dictionary from alembic.ini
    # Provide an empty dict default if the section doesn't exist (unlikely for [alembic])
    configuration = config.get_section(config.config_ini_section, {})

    # Attempt to get URL from environment variable
    db_url = get_url()  # Uses os.getenv("DB_CONNECTION_STRING")

    # If the environment variable is set, use it for sqlalchemy.url
    # This will add it to the dictionary or overwrite any value from alembic.ini
    if db_url:
        configuration['sqlalchemy.url'] = db_url
    # Check if we have a URL either from env var or potentially from the ini file
    # (though we recommended commenting it out in ini)
    elif not configuration.get('sqlalchemy.url'):
        # If ENV VAR is missing AND url is missing/commented in ini
        raise ValueError(
            "Database URL not found. Set the DB_CONNECTION_STRING environment "
            "variable or uncomment/set sqlalchemy.url in alembic.ini."
        )
    else:
        # This case means URL came from alembic.ini because env var was not set
        logging.info("Using sqlalchemy.url from alembic.ini (DB_CONNECTION_STRING env var not found).")

    # **** THIS IS THE CRITICAL PART ****
    # Create the engine using the 'configuration' dictionary we prepared.
    # DO NOT call config.get_section again here.
    connectable = engine_from_config(configuration, poolclass=pool.NullPool)
    # **** END CRITICAL PART ****

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


# Determine whether to run offline or online
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
