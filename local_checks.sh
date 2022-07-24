echo "---  python -m pytest ."
 python -m pytest .
echo "---  python -m pytest --cov='.'"
 python -m pytest --cov="."
echo "---  python -m flake8 ."
 python -m flake8 .
echo "---  python -m black . --check"
 python -m black . --check
echo "---  python -m isort . --check-only"
 python -m isort . --check-only
echo "---  bandit -r ."
 bandit -r .
#echo "--- docker-compose exec safety check"
#docker-compose exec safety check
#echo "--- docker-compose exec safety check -r requirements.txt"
#docker-compose exec safety check -r requirements.txt
#echo "--- docker-compose exec safety check -r requirements-dev.txt"
#docker-compose exec safety check -r requirements-dev.txt
#echo "--- trivy security check web"
#trivy image fastapi-testdriven_web
#echo "--- trivy security check web_db"
#trivy image fastapi-testdriven_web-db