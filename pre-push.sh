echo "export requirements.txt"
poetry export -o requirements.txt --without-hashes
poetry export -o requirements-dev.txt --dev --without-hashes
echo "autoflake"
autoflake --recursive --in-place  \
        --remove-unused-variables \
        --remove-all-unused-imports  \
        fastapi_users_db_sqlalchemy_orm tests
echo "black"
black fastapi_users_db_sqlalchemy_orm tests
echo "isort"
isort fastapi_users_db_sqlalchemy_orm tests
echo "flake8"
flake8 fastapi_users_db_sqlalchemy_orm tests --count --statistics
echo "OK"