import pytest
from unittest.mock import patch
from pydantic import ValidationError
from pytransflow.core.configuration import TransflowConfiguration
from pytransflow.core.flow import Flow
from pytransflow.core.flow.variables import FlowVariables
from pytransflow.core.flow.dataset import FailedDataset
from pytransflow.exceptions import (
    OutputAlreadyExistsException,
    FlowFailScenarioException,
    FlowFailScenarioValueNotProperlyDefinedException,
    FlowInstantFailException,
    FlowConfigurationException,
    FlowConfigurationFileNotFoundException,
    FlowSchemaNotProperlyDefinedException,
    FlowFailedException,
    FlowVariableDoesNotExistException,
    FlowVariableAlreadyExistsException,
)


def test_flow_configuration_exception():
    with pytest.raises(
        FlowConfigurationException,
        match="Flow requires 'name' or 'config' arguments",
    ):
        Flow()


def test_flow_load_yml():
    Flow(name="flow")


def test_flow_fail_validatio_yml():
    with pytest.raises(
        FlowSchemaNotProperlyDefinedException,
        match="Flow schema for flow 'flow_validation_fail'is not properly defined, please check the .yaml file.",
    ):
        Flow(name="flow_validation_fail")


def test_flow_yml_not_found():
    with pytest.raises(
        FlowConfigurationFileNotFoundException,
        match="Flow configuration file for '.*' not found",
    ):
        Flow(name="test")


def test_simple_flow():
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {"default": [{"a": "b"}]}
    assert failed_records == []


def test_simple_flow_multiple_records():
    records = [
        {},
        {"1": "a"},
        {"1": "a", "2": "b"},
        {"3": [1, 2]},
        {"4": {"5": ["a"]}}
    ]
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                },
            },
            {
                "add_field": {
                    "name": "c/d",
                    "value": "e"
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process(records)
    dataset = flow.datasets
    failed_records = flow.failed_records
    processed_records = [{**x, "a": "b", "c": {"d": "e"}} for x in records]
    assert dataset == {"default": processed_records}
    assert failed_records == []


def test_condition_processing():
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "condition": "@c/d == 'B'"
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{}, {"c": {"d": "B"}}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {"default": [{}, {"c": {"d": "B"}, "a": "b"}]}
    assert failed_records == []


def test_condition_flow_variable():
    config = {
        "variables": {
            "a": "B"
        },
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "condition": "@c/d == !:a"
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{}, {"c": {"d": "B"}}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {"default": [{}, {"c": {"d": "B"}, "a": "b"}]}
    assert failed_records == []


def test_condition_flow_variable_non_string():
    config = {
        "variables": {
            "a": 1
        },
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "condition": "@c/d == !:a"
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{}, {"c": {"d": 1}}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {"default": [{}, {"c": {"d": 1}, "a": "b"}]}
    assert failed_records == []


def test_multiple_datasets():
    records = [
        {},
        {"1": "a"},
        {"1": "a", "2": "b"},
        {"3": [1, 2]},
        {"4": {"5": ["a"]}}
    ]
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "input_datasets": ["default", "test"],
                    "output_datasets": ["a", "b"]
                }
            },
            {
                "add_field": {
                    "name": "c",
                    "value": "d",
                    "input_datasets": ["a"],
                    "output_datasets": ["c", "d"]
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process(records)
    dataset = flow.datasets
    failed_records = flow.failed_records
    processed_records = [{**x, "a": "b", "c": "d"} for x in records]
    assert dataset == {
        "b": [{**x, "a": "b"} for x in records],
        "c": processed_records,
        "d": processed_records
    }
    assert failed_records == []


def test_failed_record():
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{"a": "c"}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {}
    assert len(failed_records) == 1

    failed_record = failed_records[0]
    assert isinstance(failed_record, FailedDataset)
    assert failed_record.record == {"a": "c"}
    assert len(failed_record.failed_records) == 1
    assert isinstance(failed_record.failed_records[0].error, OutputAlreadyExistsException)


def test_one_failed_record():
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{}, {"a": "c"}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {"default": [{"a": "b"}]}

    assert len(failed_records) == 1
    failed_record = failed_records[0]
    assert isinstance(failed_record, FailedDataset)
    assert failed_record.record == {"a": "c"}
    assert len(failed_record.failed_records) == 1


def test_ignore_error():
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "ignore_errors": ["output_already_exists"]
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{}, {"a": "c"}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {"default": [{"a": "b"}, {"a": "b"}]}
    assert len(failed_records) == 0


def test_ignore_error():
    config = {
        "transformations": [
            {
                "prefix": {
                    "field": "a",
                    "value": "b",
                    "ignore_errors": ["field_does_not_exist"]
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{"c": "d"}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {"default": [{"c": "d"}]}
    assert len(failed_records) == 0


def test_fail_scenario_invalid_choices():
    inputs = [
        (
            {
                "fail_scenarios": {
                    "number_of_failed_records": -1,
                },
            },
            "Flow Fail Scenario 'number_of_failed_records' not defined properly"
        ),
        (
            {
                "fail_scenarios": {
                    "percentage_of_failed_records": -1,
                },
            },
            "Flow Fail Scenario 'percentage_of_failed_records' not defined properly"
        ),
        (
            {
                "fail_scenarios": {
                    "datasets_present": [],
                },
            },
            "Flow Fail Scenario 'datasets_present' not defined properly"
        ),
        (
            {
                "fail_scenarios": {
                    "datasets_not_present": [],
                },
            },
            "Flow Fail Scenario 'datasets_not_present' not defined properly"
        )
    ]

    for config, error in inputs:
        with pytest.raises(
            FlowFailScenarioValueNotProperlyDefinedException,
            match=error,
        ):
            Flow(config={**config, "transformations": []})


def test_fail_scenario_number_of_failed_records():
    config = {
        "fail_scenarios": {
            "number_of_failed_records": 1,
        },
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                }
            }
        ]
    }
    flow = Flow(config=config)
    with pytest.raises(
        FlowFailScenarioException,
        match="Flow Fail Scenario 'number_of_failed_records': Threshold defined as 1, got 1"
    ):
        flow.process([{"a": "b"}])


def test_fail_scenario_percentage_of_failed_records():
    config = {
        "fail_scenarios": {
            "percentage_of_failed_records": 50,
        },
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                }
            }
        ]
    }
    flow = Flow(config=config)
    with pytest.raises(
        FlowFailScenarioException,
        match="Flow Fail Scenario 'percentage_of_failed_records': Threshold defined as 50, got 50"
    ):
        flow.process([{}, {"a": "b"}])


def test_fail_scenario_datasets_present():
    config = {
        "fail_scenarios": {
            "datasets_present": ['a'],
        },
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "output_datasets": ['a']
                }
            }
        ]
    }
    flow = Flow(config=config)
    with pytest.raises(
        FlowFailScenarioException,
        match="Flow Fail Scenario 'datasets_present': Dataset 'a' is present"
    ):
        flow.process([{}])


def test_fail_scenario_datasets_not_present():
    config = {
        "fail_scenarios": {
            "datasets_not_present": ['b'],
        },
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "output_datasets": ['a']
                }
            }
        ]
    }
    flow = Flow(config=config)
    with pytest.raises(
        FlowFailScenarioException,
        match="Flow Fail Scenario 'datasets_not_present': Dataset 'b' is not present"
    ):
        flow.process([{}])


def test_condition_syntax_error():
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "condition": "315131 =~ 'D'"
                }
            }
        ]
    }
    flow = Flow(config=config)
    with pytest.raises(
        FlowFailedException,
        match=(
            "Flow failed, error: Unexpected Controller Transformation Failure, error: Condition "
            "syntax '315131 =~ 'D'' is not defined properly"
        )
    ):
        flow.process([{"c": "d"}])


def test_condition_output_dataset():
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "output_datasets": [
                        {"a": "@c == 'd'"},
                        {"name": "b", "condition": "'c' not in record"}
                    ]
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{}, {"c": "d"}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {
        "a": [{"a": "b", "c": "d"}],
        "b": [{"a": "b"}]
    }
    assert failed_records == []


def test_condition_output_dataset_fail():
    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "output_datasets": [
                        {"x": "b", "condition": "'c' not in record"}
                    ]
                }
            }
        ]
    }
    with pytest.raises(
        ValidationError,
        match=r"1 validation error for AddFieldTransformationSchema\noutput_datasets\n .*",
    ):
        Flow(config=config)


def test_instant_fail():
    config = {
        "instant_fail": True,
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                }
            }
        ]
    }
    flow = Flow(config=config)
    with pytest.raises(
        FlowInstantFailException,
        match="Flow raised instant fail exception",
    ):
        flow.process([{"a": "b"}])


def test_simple_parallel():
    records = [
        {},
        {"1": "a"},
        {"1": "a", "2": "b"},
        {"3": [1, 2]},
        {"4": {"5": ["a"]}}
    ]
    config = {
        "parallel": True,
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                },
            },
            {
                "add_field": {
                    "name": "c/d",
                    "value": "e"
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process(records)
    dataset = flow.datasets
    failed_records = flow.failed_records
    processed_records = [{**x, "a": "b", "c": {"d": "e"}} for x in records]
    assert dataset == {"default": processed_records}
    assert failed_records == []


def test_parallel_with_batch_cores():
    records = [
        {},
        {"1": "a"},
        {"1": "a", "2": "b"},
        {"3": [1, 2]},
        {"4": {"5": ["a"]}}
    ]
    config = {
        "batch": 2,
        "cores": 2,
        "parallel": True,
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                },
            },
            {
                "add_field": {
                    "name": "c/d",
                    "value": "e"
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process(records)
    dataset = flow.datasets
    failed_records = flow.failed_records
    processed_records = [{**x, "a": "b", "c": {"d": "e"}} for x in records]
    assert dataset == {"default": processed_records}
    assert failed_records == []


def test_parallel_not_available_cores():
    config = {
        "batch": 2,
        "cores": 100,
        "parallel": True,
        "transformations": []
    }
    flow = Flow(config=config)
    with pytest.raises(
        RuntimeError,
        match=r"Desired number of cores: [\d]+, available: [\d]"
    ):
        flow.process([{}])


def test_parallel_misconfigured():
    configs = [
        (
            {
                "cores": 100,
                "transformations": []
            },
            "1 validation error for FlowSchema\n  Value error, Cores parameter cannot be "
            "set if 'parallel' is not set to 'True' .*'"
        ),
        (
            {
                "batch": 2,
                "transformations": []
            },
            "1 validation error for FlowSchema\n  Value error, Batch parameter cannot be "
            "set if 'parallel' is not set to 'True' .*'"
        ),
        (
           {
                "batch": -1,
                "cores": 100,
                "parallel": True,
                "transformations": []
            },
            "1 validation error for FlowSchema\n  Value error, Batch parameter "
            "has to be greater than 0 .*"
        ),
        (
           {
                "batch": 2,
                "cores": -1,
                "parallel": True,
                "transformations": []
            },
            "1 validation error for FlowSchema\n  Value error, Cores parameter "
            "has to be greater than 0 .*"
        )
    ]
    for config, error in configs:
        with pytest.raises(
            ValueError,
            match=error,
        ):
            Flow(config=config)


def test_flow_path_separator_config():
    config = {
        "path_separator": ".",
        "transformations": [
            {
                "add_field": {
                    "name": "a.b",
                    "value": "c",
                }
            }
        ]
    }
    flow = Flow(config=config)
    flow.process([{}])
    dataset = flow.datasets
    failed_records = flow.failed_records

    assert dataset == {"default": [{"a": {"b": "c"}}]}
    assert failed_records == []
    TransflowConfiguration().path_separator = "/"


def test_flow_variables():
    variables = FlowVariables()
    variables.set_variable("a", "b")

    with pytest.raises(
        FlowVariableAlreadyExistsException,
        match="Flow variable 'a' already exists",
    ):
        variables.set_variable("a", "d")

    assert variables.get_variable("a") == "b"
    variables.update_variable("a", "c")
    assert variables.get_variable("a") == "c"
    variables.delete_variable("a")

    with pytest.raises(
        FlowVariableDoesNotExistException,
        match="Flow variable 'a' doesn't exist",
    ):
        variables.get_variable("a")

    with pytest.raises(
        FlowVariableDoesNotExistException,
        match="Flow variable 'a' doesn't exist",
    ):
        variables.update_variable("a", "d")

    with pytest.raises(
        FlowVariableDoesNotExistException,
        match="Flow variable 'a' doesn't exist",
    ):
        variables.delete_variable("a")


@patch("pytransflow.core.flow.parallel.os.cpu_count")
def test_cpu_count_none(mock):
    mock.return_value = None
    config = {
        "parallel": True,
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                },
            },
        ]
    }
    flow = Flow(config=config)
    with pytest.raises(
        RuntimeError,
        match="Cannot determine number of available cores",
    ):
        flow.process([{}])
