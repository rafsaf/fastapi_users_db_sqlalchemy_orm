# flake8: noqa
import sys

try:
    from fastapi_users_db_sqlalchemy_orm import SQLAlchemyORMUserDatabase
except:
    sys.exit(1)

sys.exit(0)
