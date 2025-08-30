import sys, os
import pytest
from fastapi.testclient import TestClient

# Caminho raiz do projeto (…/teste)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Caminho da pasta app (…/teste/app)
APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

# agora funciona tanto "from app.main" quanto "from router.rabbit"
from app.main import app

@pytest.fixture
def client():
    return TestClient(app)
