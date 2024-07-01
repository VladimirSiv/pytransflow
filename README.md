# pytransflow

A simple library for record-level processing using flows of transformations defined as YAML files

## Overview

pytransflow lets you process records by defining a flow of transformations.
Each flow has its configuration which is defined using YAML files and can be as simple as

```yaml
description: A simple test flow
instant_fail: True
fail_scenarios:
  percentage_of_failed_records: 90
variables:
  a: B
transformations:
  - prefix:
      field: a
      value: test
      condition: "@a/c/d/e == !:a"
      ignore_errors:
        - output_already_exists
      output_datasets:
        - k
  - add_field:
      name: test/a/b
      value: { "a": "b" }
      input_datasets:
        - k
      output_datasets:
        - x
        - z
```

Processing is initiated using the `Flow` class:

```python
from pytransflow.core import Flow
records = [...]

flow = Flow(name="<flow-name>")
flow.process(records)
pprint(flow.datasets)  # End result
pprint(flow.failed_records)  # Failed records
```

Refer to the [Getting Started](https://github.com/VladimirSiv/pytransflow/wiki/Getting-Started)
wiki page for additional examples and guided initial steps or check out the blog post that
introduces this library [pytransflow](https://www.vladsiv.com/pytransflow/).

## Features

The following are some of the features that pytransflow provides:

- Define processing flows using YAML files
- Use all kinds of flow configurations to fine-tune the flow
- Leverage [pydantic](https://github.com/pydantic/pydantic)‘s features for data validation
- Apply transformations only if defined condition is met
- Build your own library of transformations
- Use multiple input and output datasets
- Ignore specific errors during processing
- Set conditions for output datasets
- Track failed records
- Define flow fail scenarios
- Process records in parallel
- Use flow level variables etc.

For more information on these features and how to use them, please refer to the
[Wiki Page](https://github.com/VladimirSiv/pytransflow/wiki).

## License

MIT
