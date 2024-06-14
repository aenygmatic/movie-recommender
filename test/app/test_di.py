from unittest.mock import Mock

import pytest

from app.di import StatefulService, ServiceState


@pytest.fixture
def stateful_service():
    pre_init_criteria_mock = Mock(return_value=False)
    pre_init_function_mock = Mock()
    init_function_mock = Mock(return_value='initialized_instance')

    return StatefulService(
        pre_init_criteria=pre_init_criteria_mock,
        pre_init_function=pre_init_function_mock,
        init_function=init_function_mock
    )


def test_initial_state(stateful_service):
    assert stateful_service.state == ServiceState.NOT_AVAILABLE


def test_fast_init_when_pre_init_criteria_false(stateful_service):
    stateful_service.pre_init_criteria.return_value = False

    stateful_service.fast_init()

    assert stateful_service.instance == 'initialized_instance'
    assert stateful_service.state == ServiceState.AVAILABLE
    stateful_service.init_function.assert_called_once()


def test_fast_init_when_pre_init_criteria_true(stateful_service):
    stateful_service.pre_init_criteria.return_value = True

    stateful_service.fast_init()

    assert stateful_service.instance is None
    assert stateful_service.state == ServiceState.NOT_AVAILABLE
    stateful_service.init_function.assert_not_called()


def test_init_with_pre_init_criteria_false(stateful_service):
    stateful_service.pre_init_criteria.return_value = False

    stateful_service.init()

    assert stateful_service.instance == 'initialized_instance'
    assert stateful_service.state == ServiceState.AVAILABLE
    stateful_service.init_function.assert_called_once()
    stateful_service.pre_init_function.assert_not_called()


def test_init_with_pre_init_criteria_true(stateful_service):
    stateful_service.pre_init_criteria.return_value = True

    stateful_service.init(arg1='value1')

    assert stateful_service.instance == 'initialized_instance'
    assert stateful_service.state == ServiceState.AVAILABLE
    stateful_service.pre_init_function.assert_called_once_with({'arg1': 'value1'})
    stateful_service.init_function.assert_called_once()


def test_repeated_init_does_not_reinitialize(stateful_service):
    stateful_service.init()
    stateful_service.init()

    assert stateful_service.instance == 'initialized_instance'
    assert stateful_service.state == ServiceState.AVAILABLE
    stateful_service.init_function.assert_called_once()
