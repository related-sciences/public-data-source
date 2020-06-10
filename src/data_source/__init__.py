import os
from pathlib import Path 
from dotenv import dotenv_values

PKG_DIR = Path(__file__).absolute()
REPO_DIR = PKG_DIR.parent.parent.parent

for k, v in dotenv_values(verbose=True).items():
    globals()[k] = v
    os.environ[k] = v
