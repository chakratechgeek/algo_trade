apt-get update

# Install Python3 and pip if not present
apt-get install -y python3 python3-pip python3-venv

# Install development tools for building Python modules (often required)
apt-get install -y build-essential libpq-dev

# Install pandas, scikit-learn, xgboost, psycopg2 via apt
apt-get install -y python3-pandas python3-sklearn python3-xgboost python3-psycopg2
