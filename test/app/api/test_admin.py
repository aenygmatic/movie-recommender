from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.admin import api
from app.di import ServiceState, StatefulService


@pytest.fixture
def movie_recommender():
    return Mock()


@pytest.fixture
def stateful_service(movie_recommender):
    pre_init_criteria_mock = Mock(return_value=False)
    pre_init_function_mock = Mock()
    init_function_mock = Mock(return_value=movie_recommender)
    service = StatefulService(
        pre_init_criteria=pre_init_criteria_mock,
        pre_init_function=pre_init_function_mock,
        init_function=init_function_mock
    )
    service.state = ServiceState.NOT_AVAILABLE  # Set initial state for testing
    return service


@pytest.fixture
def app(stateful_service):
    app = FastAPI()
    app.include_router(api(stateful_service), prefix="/admin")
    return app


@pytest.fixture
def client(app):
    return TestClient(app)


def test_get_status_initial(client):
    response = client.get("/admin/status")

    assert response.status_code == 200
    assert response.json() == {"status": "NOT_AVAILABLE"}


def test_initialize_service(client, stateful_service):
    response = client.post("/admin/init", json={"dataset": "ml-100k"})

    assert response.status_code == 200
    assert response.json() == {"status": "AVAILABLE"}
    stateful_service.init_function.assert_called_once_with()


def test_get_status_after_init(client):
    client.post("/admin/init", json={"dataset": "ml-100k"})

    response = client.get("/admin/status")
    assert response.status_code == 200
    assert response.json() == {"status": "AVAILABLE"}
