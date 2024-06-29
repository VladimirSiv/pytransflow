from pytransflow.core.record import Record, FailedRecord
from pytransflow.core.flow.dataset import Datasets
from pytransflow.core.flow.pipeline import FlowPipelineState
from pytransflow.core.transformation import TransformationConfiguration
from pytransflow.transformations.add_field import AddFieldTransformationSchema


def test_datasets_init():
    datasets = Datasets()
    assert datasets.datasets == {}
    assert datasets.failed_records == []


def test_add_failed_records():
    datasets = Datasets()
    record = Record({"a": 1})
    failed_record = FailedRecord(
        record=record,
        transformation_name="test",
        transformation_configuration=TransformationConfiguration(AddFieldTransformationSchema, {"name": "a", "value": "b"},),
        error=Exception("test_error"),
    )
    state = FlowPipelineState("test-id", record)
    state.add_failed_record(failed_record)
    datasets.add_failed_records(state)
    output = datasets.failed_records[0]
    assert output.run_id == state.run_id
    assert output.record == record
    assert output.failed_records == [failed_record]


def test_add_initial_records():
    datasets = Datasets()
    records = [{"a": 1}, {"b": 2}]
    datasets.add_input_records(records)
    assert datasets.input_records == [Record(x) for x in records]


def test_add_to_dataset():
    datasets = Datasets()
    records = [{"a": 1}, {"b": 2}]
    datasets.add_to_dataset("test", records)
    assert datasets.datasets == {"test": records}
