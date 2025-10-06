import os
from pathlib import Path
from dotenv import load_dotenv


# directory from where app was executed
BASE_DIR = os.getcwd()

# path to both local .env and shared .env
PROJECT_ENV = os.path.abspath(os.path.join(BASE_DIR, ".env"))
ROOT_ENV = os.path.abspath(os.path.join(BASE_DIR, "..", ".env.shared"))

print(ROOT_ENV) # Show path th ROOT_ENV file
print(PROJECT_ENV) # Show path to PROJECT_ENV file


# Load shared env first
load_dotenv(ROOT_ENV)

# Load project-specific env 
# Overrides shared entry if the given entry is in both
load_dotenv(PROJECT_ENV, override=True)
