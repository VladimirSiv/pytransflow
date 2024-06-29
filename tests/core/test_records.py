import pytest
from pytransflow.core.record import FailedRecord, Record
from pytransflow.exceptions.records import RecordAddException
from pytransflow.transformations.rename import RenameTransformationSchema
from pytransflow.core.transformation import TransformationConfiguration


def test_record_initialization():
    data = {"a": "b"}
    record = Record(data)

    assert record.data == data


@pytest.mark.parametrize(
    "value, result",
    [
        ("a", False),
        (1, False),
        (True, False),
        (False, False),
        ({"a": "b"}, True),
        ({"a": "c"}, False),
        ((1, 2), False),
    ],
)
def test_record_equal(value, result):
    record = Record({"a": "b"})
    assert (value == record) == result


def test_record_repr():
    value = {"a": "b"}
    record = Record(value)
    assert repr(record) == repr(value)


def test_record_iteration():
    data = {"a": 1, "b": 2, "c": 3, "d": 4}
    record = Record(data)
    keys = [x for x in record]

    assert keys == list(data.keys())


def test_record_fields():
    data = {"a": 1, "b": 2, "c": 3, "d": 4}
    record = Record(data)

    assert list(data.keys()) == list(record.fields())


def test_record_get_item():
    data = {"a": "b"}
    record = Record(data)

    assert record["a"] == "b"


def test_record_set_item():
    data = {"a": "b"}
    record = Record(data)
    record["c"] = "d"

    assert record.data == {"a": "b", "c": "d"}


@pytest.mark.parametrize(
    "data, path, result",
    [
        ({"a": "b"}, "a", True),
        ({"a": {"b": 1}}, "a/b", True),
        ({"a": {"b": {"c": {"d": [1, 2]}}}}, "a/b/c/d", True),
        ({"a": {"b": {"c": {"d": [1, 2]}}}}, "a/b/d", False),
        ({"a": {"b": {"c": {"d": [1, 2]}}}}, "a/b/c/d/e", False),
    ],
)
def test_record_contains(data, path, result):
    record = Record(data)
    assert record.contains(path) == result


@pytest.mark.parametrize(
    "data, path, value, result",
    [
        ({"a": "b"}, "b", 1, {"a": "b", "b": 1}),
        ({"a": "b"}, "b/c/d", "1", {"a": "b", "b": {"c": {"d": "1"}}}),
    ],
)
def test_record_add(data, path, value, result):
    record = Record(data)
    record.add(path, value)
    assert record == result


@pytest.mark.parametrize(
    "data, path, value",
    [
        # TODO: Should this raise an error?
        # The value i.e. path is already present in the record,
        # should we overwrite it?
        # ({"a": "b"}, "a", 1),
        ({"a": "b"}, "a/b", "1"),
        ({"a": [1, 2]}, "a/b/c", "1"),
    ],
)
def test_record_add(data, path, value):
    record = Record(data)
    with pytest.raises(RecordAddException):
        record.add(path, value)


def test_record_remove_required():
    record = Record({"a": "b"})
    path = "b/c"
    error = f"Element at path '{path}' is not contained in the record"
    with pytest.raises(ValueError) as err:
        record.remove(path, is_required=True)
    assert str(err.value) == error


def test_record_remove_not_required():
    record = Record({"a": "b"})
    path = "b/c"
    record.remove(path, is_required=False)
    assert record == record


def test_record_items():
    value = {"a": "b", "c": "d"}
    record = Record(value)
    assert list(value.items()) == list(record.items())


def test_record_values():
    data = {"a": "b", "c": "d"}
    record = Record(data)
    assert list(record.values()) == list(data.values())


def test_initialization_and_repr():
    record = {"a": "b"}
    transformation_name = "test"
    transformation_configuration = {"c": "d"}
    error = Exception("test error")

    failed_record = FailedRecord(
        record=record,
        transformation_name=transformation_name,
        transformation_configuration=transformation_configuration,
        error=error,
    )

    assert failed_record.record == record
    assert failed_record.transformation_name == transformation_name
    assert failed_record.transformation_configuration == transformation_configuration
    assert failed_record.error == error
    assert repr(failed_record) == (
        f"FailedRecord(record={repr(record)}, "
        f"transformation_name={transformation_name}, "
        f"transformation_configuration={transformation_configuration}, "
        f"error={error})"
    )


def test_equal_failed_records():
    failed_record_1 = FailedRecord(
        record={"a": "b"},
        error="Field 'b' does not exist",
        transformation_name="Rename",
        transformation_configuration=TransformationConfiguration(
            RenameTransformationSchema,
            {
                "field": "b",
                "output": "c",
                "input_datasets": ["default"],
                "output_datasets": ["default"],
                "condition": None,
                "output_fields": ["output"],
                "required_in_record": ["b"],
                "ignore_errors": [],
            },
        ),
    )

    failed_record_2 = FailedRecord(
        record={"a": "b"},
        error="Field 'b' does not exist",
        transformation_name="Rename",
        transformation_configuration=TransformationConfiguration(
            RenameTransformationSchema,
            {
                "field": "b",
                "output": "c",
                "input_datasets": ["default"],
                "output_datasets": ["default"],
                "condition": None,
                "output_fields": ["output"],
                "required_in_record": ["b"],
                "ignore_errors": [],
            },
        ),
    )

    assert failed_record_1 == failed_record_2


@pytest.mark.parametrize(
    "value",
    ["a", 1, True, False, {"a": "b"}, (1, 2)],
)
def test_equal_failed_record(value):
    failed_record = FailedRecord(
        record={"a": "b"},
        error="Field 'b' does not exist",
        transformation_name="Rename",
        transformation_configuration=TransformationConfiguration(
            RenameTransformationSchema,
            {
                "field": "b",
                "output": "c",
                "input_datasets": ["default"],
                "output_datasets": ["default"],
                "condition": None,
                "output_fields": ["output"],
                "required_in_record": ["b"],
                "ignore_errors": [],
            },
        ),
    )
    assert value != failed_record
