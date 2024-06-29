import pytest
from pytransflow.core import Flow
from pytransflow.core.eval import SimpleEval


def test_custom_eval_function():
    def double(x):
        return x*x

    SimpleEval.add_function(
        name="double",
        function=double
    )

    config = {
        "transformations": [
            {
                "add_field": {
                    "name": "a",
                    "value": "b",
                    "condition": "double(@c) == 100"
                },
            },
        ]
    }
    flow = Flow(config=config)
    flow.process([{"c": 10}])
    datasets = flow.datasets
    assert datasets == {"default": [{"a": "b", "c": 10}]}
    assert len(flow.failed_records) == 0


def test_custom_function_not_str():
    def double(x):
        return x*x

    with pytest.raises(
        RuntimeError,
        match="Function name should be of type <str>",
    ):
        SimpleEval.add_function(
            name=1,
            function=double
        )


def test_custom_function_not_call():

    with pytest.raises(
        RuntimeError,
        match="Function object is not callable, hence it cannot be used for evaluating.",
    ):
        SimpleEval.add_function(
            name="double",
            function="double"
        )
