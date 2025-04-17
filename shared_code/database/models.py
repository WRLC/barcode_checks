"""Models for SQLAlchemy ORM."""

import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, ForeignKey, DateTime, UniqueConstraint, Text, Table, text
)
from sqlalchemy.orm import relationship, declarative_base, Mapped, mapped_column
from sqlalchemy.sql import func
from typing import List, Optional, Set

# Define the base class for declarative models
Base = declarative_base()


# --- Association Object Classes ---

class ApiKeyPermission(Base):
    """Association object for linking API keys to Alma API permissions.

    This allows for a many-to-many relationship between API keys and permissions,
    with additional attributes to specify read-only access.

    Attributes:
        id (int): Primary key for the association.
        api_key_id (int): Foreign key referencing the API key.
        permission_id (int): Foreign key referencing the Alma API permission.
        is_read_only (bool): Indicates if the permission is read-only.
        created_at (datetime): Timestamp of when the association was created.

    """

    __tablename__ = 'api_key_permissions'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    api_key_id: Mapped[int] = mapped_column(ForeignKey("api_keys.id"), nullable=False)
    permission_id: Mapped[int] = mapped_column(ForeignKey("alma_api_permissions.id"), nullable=False)
    is_read_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("1"))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Constraints
    __table_args__ = (
        UniqueConstraint('api_key_id', 'permission_id', name='uq_api_key_permission'),
    )

    # Relationships
    api_key: Mapped["APIKey"] = relationship(back_populates="permission_links")
    permission: Mapped["AlmaApiPermission"] = relationship(back_populates="api_key_links")

    def __repr__(self):
        """String representation of the ApiKeyPermission object."""

        return f"<ApiKeyPermission(key={self.api_key_id}, perm={self.permission_id}, ro={self.is_read_only})>"


class IZAnalysisConnector(Base):
    """Association object for linking Institution Zones to Analyses.

    This allows for a many-to-many relationship between institution zones and analyses,
    with additional attributes to specify the analysis path.

    Attributes:
        id (int): Primary key for the association.
        institution_zone_id (int): Foreign key referencing the institution zone.
        analysis_id (int): Foreign key referencing the analysis.
        analysis_path (str): Path to the analysis.
        created_at (datetime): Timestamp of when the association was created.
        updated_at (datetime): Timestamp of when the association was last updated.

    """
    __tablename__ = 'iz_analysis_connector'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    institution_zone_id: Mapped[int] = mapped_column(ForeignKey('institution_zones.id'), nullable=False)
    analysis_id: Mapped[int] = mapped_column(ForeignKey('analyses.id'), nullable=False)
    analysis_path: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), server_default=func.now()
    )

    # Constraints
    __table_args__ = (
        UniqueConstraint(  # institution_zone_id and analysis_path must be unique together
            'institution_zone_id', 'analysis_path', name='uq_iz_analysis_path',
        ),
        UniqueConstraint(  # institution_zone_id and analysis_id must be unique together
            'institution_zone_id', 'analysis_id', name='uq_iz_analysis_connector'
        )
    )

    # Relationships
    institution_zone: Mapped["InstitutionZone"] = relationship(back_populates="iz_analysis_connectors")
    analysis: Mapped["Analysis"] = relationship(back_populates="iz_analysis_connectors")
    trigger_config_links: Mapped[List["TriggerConfigIZAnalysisLink"]] = relationship(
        back_populates="iz_analysis_connector"
    )

    def __repr__(self):
        """String representation of the IZAnalysisConnector object."""

        return (f"<IZAnalysisConnector(id={self.id}, iz_id={self.institution_zone_id}, analysis_id={self.analysis_id}, "
                f"path='{self.analysis_path}')>")


class TriggerConfigIZAnalysisLink(Base):
    """Association object for linking Timer Trigger Configurations to Analyses.

    This allows for a many-to-many relationship between trigger configurations and analyses.

    Attributes:
        id (int): Primary key for the association.
        trigger_config_id (int): Foreign key referencing the Timer Trigger Configuration.
        iz_analysis_connector_id (int): Foreign key referencing the IZAnalysisConnector.
        created_at (datetime): Timestamp of when the association was created.

    """
    __tablename__ = 'trigger_config_iz_analysis_link'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trigger_config_id: Mapped[int] = mapped_column(ForeignKey('timer_trigger_configs.id'), nullable=False)
    iz_analysis_connector_id: Mapped[int] = mapped_column(ForeignKey('iz_analysis_connector.id'), nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Constraints
    __table_args__ = (  # trigger_config_id and iz_analysis_connector_id must be unique together
        UniqueConstraint('trigger_config_id', 'iz_analysis_connector_id', name='uq_trigger_iz_analysis_link'),
    )

    # Relationships
    trigger_config: Mapped["TimerTriggerConfig"] = relationship(back_populates="iz_analysis_links")
    iz_analysis_connector: Mapped["IZAnalysisConnector"] = relationship(back_populates="trigger_config_links")

    def __repr__(self):
        """String representation of the TriggerConfigIZAnalysisLink object."""

        return (f"<TriggerConfigIZAnalysisLink(trigger={self.trigger_config_id}, "
                f"iz_analysis={self.iz_analysis_connector_id})>")


class TriggerConfigUserLink(Base):
    """Association object for linking Timer Trigger Configurations to Users.

    This allows for a many-to-many relationship between trigger configurations and users.

    Attributes:
        id (int): Primary key for the association.
        trigger_config_id (int): Foreign key referencing the Timer Trigger Configuration.
        user_id (int): Foreign key referencing the User.
        created_at (datetime): Timestamp of when the association was created.

    """
    __tablename__ = 'trigger_config_user_link'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trigger_config_id: Mapped[int] = mapped_column(ForeignKey('timer_trigger_configs.id'), nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'), nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Constraints
    __table_args__ = (  # trigger_config_id and user_id must be unique together
        UniqueConstraint('trigger_config_id', 'user_id', name='uq_trigger_user_link'),
    )

    # Relationships
    trigger_config: Mapped["TimerTriggerConfig"] = relationship(back_populates="user_notification_links")
    user: Mapped["User"] = relationship(back_populates="trigger_config_links")

    def __repr__(self):
        """String representation of the TriggerConfigUserLink object."""

        return f"<TriggerConfigUserLink(trigger={self.trigger_config_id}, user={self.user_id})>"


# --- Main Entity Tables ---

class TimerTriggerConfig(Base):
    """Configuration for Timer Trigger.

    This table stores the configuration for timer triggers, including the schedule,
    email subject, and the type of handler to be used.

    Attributes:
        id (int): Primary key for the configuration.
        config_name (str): Name of the configuration.
        description (str): Description of the configuration.
        schedule_cron (str): Cron expression for the schedule.
        email_subject (str): Subject of the email to be sent.
        dataprep_handler_type (str): Type of handler to be used.
        is_active (bool): Indicates if the configuration is active.
        created_at (datetime): Timestamp of when the configuration was created.
        updated_at (datetime): Timestamp of when the configuration was last updated.

    """
    __tablename__ = 'timer_trigger_configs'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    config_name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    schedule_cron: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g., "0 0 5 * * *"
    email_subject: Mapped[str] = mapped_column(String(500), nullable=False)
    dataprep_handler_type: Mapped[str] = mapped_column(
        String(100), nullable=False
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), server_default=func.now()
    )

    # Relationships
    iz_analysis_links: Mapped[List["TriggerConfigIZAnalysisLink"]] = relationship(
        back_populates="trigger_config", cascade="all, delete-orphan"
    )
    user_notification_links: Mapped[List["TriggerConfigUserLink"]] = relationship(
        back_populates="trigger_config", cascade="all, delete-orphan"
    )

    def __repr__(self):
        """String representation of the TimerTriggerConfig object."""

        return f"<TimerTriggerConfig(id={self.id}, name='{self.config_name}', schedule='{self.schedule_cron}')>"


class InstitutionZone(Base):
    """Configuration for Institution Zones.
    This table stores the configuration for institution zones, including the code,
    name, and relationships to users and API keys.

    Attributes:
        id (int): Primary key for the institution zone.
        iz_code (str): Code for the institution zone.
        iz_name (str): Name of the institution zone.
        created_at (datetime): Timestamp of when the institution zone was created.
        updated_at (datetime): Timestamp of when the institution zone was last updated.

    """
    __tablename__ = 'institution_zones'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    iz_code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    iz_name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(
        timezone=True), onupdate=func.now(), server_default=func.now()
    )

    # Relationships
    users: Mapped[List["User"]] = relationship(back_populates="institution_zone", cascade="all, delete-orphan")
    api_keys: Mapped[List["APIKey"]] = relationship(back_populates="institution_zone", cascade="all, delete-orphan")
    iz_analysis_connectors: Mapped[List["IZAnalysisConnector"]] = relationship(
        back_populates="institution_zone", cascade="all, delete-orphan"
    )

    def __repr__(self):
        """String representation of the InstitutionZone object."""

        return f"<InstitutionZone(iz_code='{self.iz_code}', name='{self.iz_name}')>"


class Analysis(Base):
    """Configuration for Analyses.

    This table stores the configuration for analyses, including the name,
    required permissions, and relationships to institution zones and users.

    Attributes:
        id (int): Primary key for the analysis.
        analysis_name (str): Name of the analysis.
        required_permission_id (int): Foreign key referencing the required permission.
        created_at (datetime): Timestamp of when the analysis was created.
        updated_at (datetime): Timestamp of when the analysis was last updated.

    """
    __tablename__ = 'analyses'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    analysis_name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    required_permission_id: Mapped[Optional[int]] = mapped_column(ForeignKey('alma_api_permissions.id'))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), server_default=func.now()
    )

    # Relationships
    iz_analysis_connectors: Mapped[List["IZAnalysisConnector"]] = relationship(
        back_populates="analysis", cascade="all, delete-orphan"
    )
    required_permission: Mapped[Optional["AlmaApiPermission"]] = relationship(
        back_populates="required_for_analyses"
    )

    def __repr__(self):
        """String representation of the Analysis object."""

        return f"<Analysis(id={self.id}, name='{self.analysis_name}')>"


class AlmaApiPermission(Base):
    """Configuration for Alma API Permissions.

    This table stores the configuration for Alma API permissions, including the name,
    description, and relationships to API keys and analyses.

    Attributes:
        id (int): Primary key for the permission.
        permission_name (str): Name of the permission.
        description (str): Description of the permission.
        created_at (datetime): Timestamp of when the permission was created.
        updated_at (datetime): Timestamp of when the permission was last updated.

    """
    __tablename__ = 'alma_api_permissions'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    permission_name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    description: Mapped[Optional[str]] = mapped_column(String(500))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), server_default=func.now()
    )

    # Relationships
    api_key_links: Mapped[List["ApiKeyPermission"]] = relationship(
        ApiKeyPermission, back_populates="permission", cascade="all, delete-orphan"
    )
    required_for_analyses: Mapped[List["Analysis"]] = relationship(back_populates="required_permission")

    def __repr__(self):
        """String representation of the AlmaApiPermission object."""

        return f"<AlmaApiPermission(name='{self.permission_name}')>"


class User(Base):
    """Configuration for Users.
    This table stores the configuration for users, including the email,
    institution zone, and relationships to analyses and API keys.

    Attributes:
        id (int): Primary key for the user.
        user_email (str): Email of the user.
        institution_zone_id (int): Foreign key referencing the institution zone.
        created_at (datetime): Timestamp of when the user was created.
        updated_at (datetime): Timestamp of when the user was last updated.

    """
    __tablename__ = 'users'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    institution_zone_id: Mapped[Optional[int]] = mapped_column(ForeignKey('institution_zones.id'))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), server_default=func.now()
    )

    # Relationships
    institution_zone: Mapped[Optional["InstitutionZone"]] = relationship(back_populates="users")
    trigger_config_links: Mapped[List["TriggerConfigUserLink"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self):
        """String representation of the User object."""

        return f"<User(email='{self.user_email}')>"


class APIKey(Base):
    """Configuration for API Keys.

    This table stores the configuration for API keys, including the value,
    description, and relationships to institution zones and permissions.

    Attributes:
        id (int): Primary key for the API key.
        institution_zone_id (int): Foreign key referencing the institution zone.
        api_key_value (str): Value of the API key.
        description (str): Description of the API key.
        created_at (datetime): Timestamp of when the API key was created.
        updated_at (datetime): Timestamp of when the API key was last updated.

    """
    __tablename__ = 'api_keys'

    # Columns
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    institution_zone_id: Mapped[int] = mapped_column(ForeignKey('institution_zones.id'), nullable=False)
    api_key_value: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(255))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), onupdate=func.now(), server_default=func.now()
    )

    # Relationships
    institution_zone: Mapped["InstitutionZone"] = relationship("InstitutionZone", back_populates="api_keys")
    permission_links: Mapped[List["ApiKeyPermission"]] = relationship(
        back_populates="api_key", cascade="all, delete-orphan"
    )

    def __repr__(self):
        """String representation of the APIKey object."""

        return f"<APIKey(iz_id={self.institution_zone_id}, desc='{self.description}', id={self.id})>"
