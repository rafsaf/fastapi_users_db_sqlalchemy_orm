"""FastAPI Users database adapter for SQLAlchemy ORM."""
import uuid
from typing import Optional, Type

from fastapi_users.db.base import BaseUserDatabase
from fastapi_users.models import UD
from pydantic import UUID4
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    delete,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.types import CHAR, TypeDecorator


class GUID(TypeDecorator):  # pragma: no cover
    """Platform-independent GUID type.
    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(36), storing as regular strings.
    """

    class UUIDChar(CHAR):
        python_type = UUID4  # type: ignore

    impl = UUIDChar

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == "postgresql":
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return str(uuid.UUID(value))
            else:
                return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value


class SQLAlchemyORMBaseUserTable:
    """Base SQLAlchemy users table definition."""

    __tablename__ = "user"

    id = Column(GUID, primary_key=True)
    email = Column(String(length=320), unique=True, index=True, nullable=False)
    hashed_password = Column(String(length=72), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    is_superuser = Column(Boolean, default=False, nullable=False)
    is_verified = Column(Boolean, default=False, nullable=False)


class SQLAlchemyORMBaseOAuthAccountTable:
    """Base SQLAlchemy OAuth account table definition."""

    __tablename__ = "oauth_account"

    id = Column(GUID, primary_key=True)
    oauth_name = Column(String(length=100), index=True, nullable=False)
    access_token = Column(String(length=1024), nullable=False)
    expires_at = Column(Integer, nullable=True)
    refresh_token = Column(String(length=1024), nullable=True)
    account_id = Column(String(length=320), index=True, nullable=False)
    account_email = Column(String(length=320), nullable=False)

    @declared_attr
    def user_id(cls):
        return Column(GUID, ForeignKey("user.id", ondelete="cascade"), nullable=False)


class NotSetOAuthAccountTableError(Exception):
    """
    OAuth table was not set in DB adapter but was needed.
    Raised when trying to create/update a user with OAuth accounts set
    but no table were specified in the DB adapter.
    """


class SQLAlchemyORMUserDatabase(BaseUserDatabase[UD]):
    """
    Database adapter for SQLAlchemy.
    :param user_db_model: Pydantic model of a DB representation of a user.
    :param session: `AsyncSession` instance from `sqlalchemy.ext.asyncio`.
    :param users: SQLAlchemy users table instance.
    :param oauth_accounts: Optional SQLAlchemy OAuth accounts table instance.
    """

    def __init__(
        self,
        user_db_model: Type[UD],
        session: AsyncSession,
        users: SQLAlchemyORMBaseUserTable,
        oauth_accounts: SQLAlchemyORMBaseOAuthAccountTable = None,
    ):
        super().__init__(user_db_model)
        self.session = session
        self.users = users
        self.oauth_accounts = oauth_accounts

    async def get(self, id: UUID4) -> Optional[UD]:
        query = select(self.users).where(self.users.id == id)
        result = await self.session.execute(query)
        user = result.scalars().first()
        return await self._make_user(user) if user else None

    async def get_by_email(self, email: str) -> Optional[UD]:
        query = select(self.users).where(
            func.lower(self.users.email) == func.lower(email)
        )
        result = await self.session.execute(query)
        user = result.scalars().first()
        return await self._make_user(user) if user else None

    async def get_by_oauth_account(self, oauth: str, account_id: str) -> Optional[UD]:
        if self.oauth_accounts is not None:
            query = (
                select([self.users])
                .join(self.oauth_accounts)
                .where(self.oauth_accounts.oauth_name == oauth)
                .where(self.oauth_accounts.account_id == account_id)
            )
            result = await self.session.execute(query)
            user = result.scalars().first()
            return await self._make_user(user) if user else None
        raise NotSetOAuthAccountTableError()

    async def create(self, user: UD) -> UD:
        user_dict = user.dict()
        oauth_accounts_values = None

        if "oauth_accounts" in user_dict:
            oauth_accounts_values = []

            oauth_accounts = user_dict.pop("oauth_accounts")
            for oauth_account in oauth_accounts:
                oauth_accounts_values.append({"user_id": user.id, **oauth_account})

        query = insert(self.users)
        await self.session.execute(query, user_dict)
        await self.session.commit()

        if oauth_accounts_values is not None:
            if self.oauth_accounts is None:
                raise NotSetOAuthAccountTableError()
            query = insert(self.oauth_accounts)
            await self.session.execute(query, oauth_accounts_values)
            await self.session.commit()

        return user

    async def update(self, user: UD) -> UD:
        user_dict = user.dict()

        if "oauth_accounts" in user_dict:
            if self.oauth_accounts is None:
                raise NotSetOAuthAccountTableError()

            delete_query = delete(self.oauth_accounts).where(
                self.oauth_accounts.user_id == user.id
            )
            await self.session.execute(delete_query)

            oauth_accounts_values = []
            oauth_accounts = user_dict.pop("oauth_accounts")
            for oauth_account in oauth_accounts:
                oauth_accounts_values.append({"user_id": user.id, **oauth_account})

            insert_query = insert(self.oauth_accounts)
            await self.session.execute(insert_query, oauth_accounts_values)
            await self.session.commit()

        update_query = (
            update(self.users).where(self.users.id == user.id).values(user_dict)
        )
        await self.session.execute(update_query)
        await self.session.commit()
        return user

    async def delete(self, user: UD) -> None:
        query = delete(self.users).where(self.users.id == user.id)
        await self.session.execute(query)
        await self.session.commit()

    async def _make_user(self, user: SQLAlchemyORMBaseUserTable) -> UD:
        user_dict = user.__dict__
        if self.oauth_accounts is not None:
            query = select(self.oauth_accounts).where(
                self.oauth_accounts.user_id == user_dict["id"]
            )
            result = await self.session.execute(query)
            oauth_accounts = result.scalars().all()
            user_dict["oauth_accounts"] = [a.__dict__ for a in oauth_accounts]
        return self.user_db_model(**user_dict)
