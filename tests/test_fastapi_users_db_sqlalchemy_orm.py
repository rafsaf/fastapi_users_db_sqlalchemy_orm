from typing import Any, AsyncGenerator

import pytest
from sqlalchemy import Column, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker

from fastapi_users_db_sqlalchemy_orm import (
    NotSetOAuthAccountTableError,
    SQLAlchemyORMBaseOAuthAccountTable,
    SQLAlchemyORMBaseUserTable,
    SQLAlchemyORMUserDatabase,
)
from tests.conftest import UserDB, UserDBOAuth


@pytest.fixture
async def sqlalchemy_orm_user_db() -> AsyncGenerator[SQLAlchemyORMUserDatabase, None]:
    Base: Any = declarative_base()

    class User(SQLAlchemyORMBaseUserTable, Base):
        first_name = Column(String, nullable=True)

    async_engine = create_async_engine(
        "sqlite+aiosqlite:///./test-sqlalchemy-user.db", pool_pre_ping=True
    )
    async_session = sessionmaker(
        async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        yield SQLAlchemyORMUserDatabase(UserDB, session, User)


@pytest.fixture
async def sqlalchemy_orm_user_db_oauth() -> AsyncGenerator[
    SQLAlchemyORMUserDatabase, None
]:
    Base: Any = declarative_base()

    class User(SQLAlchemyORMBaseUserTable, Base):
        first_name = Column(String, nullable=True)

    class OAuthAccount(SQLAlchemyORMBaseOAuthAccountTable, Base):
        pass

    async_engine = create_async_engine(
        "sqlite+aiosqlite:///./test-sqlalchemy-user-oauth.db", pool_pre_ping=True
    )
    async_session = sessionmaker(
        async_engine, expire_on_commit=False, class_=AsyncSession
    )
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        yield SQLAlchemyORMUserDatabase(UserDBOAuth, session, User, OAuthAccount)


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries(sqlalchemy_orm_user_db: SQLAlchemyORMUserDatabase[UserDB]):
    user = UserDB(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
    )

    # Create
    user_db = await sqlalchemy_orm_user_db.create(user)
    assert user_db.id is not None
    assert user_db.is_active is True
    assert user_db.is_superuser is False
    assert user_db.email == user.email

    # Update
    user_db.is_superuser = True
    await sqlalchemy_orm_user_db.update(user_db)

    # Get by id
    id_user = await sqlalchemy_orm_user_db.get(user.id)
    assert id_user is not None
    assert id_user.id == user_db.id
    assert id_user.is_superuser is True

    # Get by email
    email_user = await sqlalchemy_orm_user_db.get_by_email(str(user.email))
    assert email_user is not None
    assert email_user.id == user_db.id

    # Get by uppercased email
    email_user = await sqlalchemy_orm_user_db.get_by_email("Lancelot@camelot.bt")
    assert email_user is not None
    assert email_user.id == user_db.id

    # Exception when inserting existing email
    with pytest.raises(IntegrityError):
        await sqlalchemy_orm_user_db.create(user)

    # Exception when inserting non-nullable fields
    with pytest.raises(IntegrityError):
        # Use construct to bypass Pydantic validation
        wrong_user = UserDB.construct(hashed_password="aaa")
        await sqlalchemy_orm_user_db.create(wrong_user)

    # Unknown user
    unknown_user = await sqlalchemy_orm_user_db.get_by_email("galahad@camelot.bt")
    assert unknown_user is None

    # Delete user
    await sqlalchemy_orm_user_db.delete(user)
    deleted_user = await sqlalchemy_orm_user_db.get(user.id)
    assert deleted_user is None

    # Exception when creating/updating a OAuth user
    user_oauth = UserDBOAuth(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
    )
    with pytest.raises(NotSetOAuthAccountTableError):
        await sqlalchemy_orm_user_db.create(user_oauth)
    with pytest.raises(NotSetOAuthAccountTableError):
        await sqlalchemy_orm_user_db.update(user_oauth)

    # Exception when trying to get by OAuth account
    with pytest.raises(NotSetOAuthAccountTableError):
        await sqlalchemy_orm_user_db.get_by_oauth_account("foo", "bar")


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries_custom_fields(
    sqlalchemy_orm_user_db: SQLAlchemyORMUserDatabase[UserDB],
):
    """It should output custom fields in query result."""
    user = UserDB(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
        first_name="Lancelot",
    )
    await sqlalchemy_orm_user_db.create(user)

    id_user = await sqlalchemy_orm_user_db.get(user.id)
    assert id_user is not None
    assert id_user.id == user.id
    assert id_user.first_name == user.first_name


@pytest.mark.asyncio
@pytest.mark.db
async def test_queries_oauth(
    sqlalchemy_orm_user_db_oauth: SQLAlchemyORMUserDatabase[UserDBOAuth],
    oauth_account1,
    oauth_account2,
):
    user = UserDBOAuth(
        email="lancelot@camelot.bt",
        hashed_password="guinevere",
        oauth_accounts=[oauth_account1, oauth_account2],
    )

    # Create
    user_db = await sqlalchemy_orm_user_db_oauth.create(user)
    assert user_db.id is not None
    assert hasattr(user_db, "oauth_accounts")
    assert len(user_db.oauth_accounts) == 2

    # Update
    user_db.oauth_accounts[0].access_token = "NEW_TOKEN"
    await sqlalchemy_orm_user_db_oauth.update(user_db)

    # Get by id
    id_user = await sqlalchemy_orm_user_db_oauth.get(user.id)
    assert id_user is not None
    assert id_user.id == user_db.id
    assert id_user.oauth_accounts[0].access_token == "NEW_TOKEN"

    # Get by email
    email_user = await sqlalchemy_orm_user_db_oauth.get_by_email(str(user.email))
    assert email_user is not None
    assert email_user.id == user_db.id
    assert len(email_user.oauth_accounts) == 2

    # Get by OAuth account
    oauth_user = await sqlalchemy_orm_user_db_oauth.get_by_oauth_account(
        oauth_account1.oauth_name, oauth_account1.account_id
    )
    assert oauth_user is not None
    assert oauth_user.id == user.id

    # Unknown OAuth account
    unknown_oauth_user = await sqlalchemy_orm_user_db_oauth.get_by_oauth_account(
        "foo", "bar"
    )
    assert unknown_oauth_user is None
