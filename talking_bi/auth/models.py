from datetime import datetime, timezone

from sqlalchemy import Column, String, DateTime, Text
from sqlalchemy import inspect, text as sql_text
from database import Base
import uuid

class Organization(Base):
    __tablename__ = "organizations"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, index=True, nullable=False)
    admin_id = Column(String, nullable=False)

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)

    role = Column(String, default="user")  # user | admin
    org_id = Column(String, nullable=True)
    display_name = Column(String, nullable=True)
    avatar_url = Column(Text, nullable=True)


class UserAPIKey(Base):
    __tablename__ = "user_api_keys"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False, index=True)
    provider = Column(String, nullable=False, index=True)
    label = Column(String, nullable=True)
    secret_masked = Column(String, nullable=False)
    secret_hash = Column(String, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


class AuthActivityLog(Base):
    __tablename__ = "auth_activity_logs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False, index=True)  # login/logout/oauth/register/password_change
    provider = Column(String, nullable=True)
    ip_address = Column(String, nullable=True)
    user_agent = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


def ensure_auth_schema(engine) -> None:
    """
    Lightweight runtime schema upgrades for existing SQLite/Postgres installs.
    """
    inspector = inspect(engine)
    if "users" in inspector.get_table_names():
        user_cols = {c["name"] for c in inspector.get_columns("users")}
        with engine.begin() as conn:
            if "display_name" not in user_cols:
                conn.execute(sql_text("ALTER TABLE users ADD COLUMN display_name VARCHAR"))
            if "avatar_url" not in user_cols:
                conn.execute(sql_text("ALTER TABLE users ADD COLUMN avatar_url TEXT"))
