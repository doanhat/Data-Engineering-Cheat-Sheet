# Data-Engineering-Cheat-Sheet

[![hackmd-github-sync-badge](https://hackmd.io/xQc-EfsyQkyd5Lt1LDxkFw/badge)](https://hackmd.io/xQc-EfsyQkyd5Lt1LDxkFw)

Cheat Sheet for working with Data as a Data Engineer.

## Table of Contents

[TOC]

## Python
### Working with nested data structure

#### 1. Convert a "Struct" string to json (python dict)

A struct string has a format as "struct<...array<>>" which can be found in AWS Glue schema or avro schema. 

```python=
def struct_to_json(struct):
    """
    Expands embedded struct strings to Python dictionaries
    Args:
        Struct format as string
    Returns
        JSON object
    """
    struct = re.sub(r'[\s]+', '', struct).strip()
    r = re.compile(r'(.*?)(struct<|array<|[:,>])(.*)')
    root = dict()

    to_parse = struct
    parents = []
    curr_elem = root

    key = None
    while to_parse:
        left, operator, to_parse = r.match(to_parse).groups()

        if operator == 'struct<' or operator == 'array<':
            parents.append(curr_elem)
            new_elem = dict() if operator == 'struct<' else list()
            if key:
                curr_elem[key] = new_elem
                curr_elem = new_elem
            elif isinstance(curr_elem, list):
                curr_elem.append(new_elem)
                curr_elem = new_elem
            key = None
        elif operator == ':':
            key = left
        elif operator == ',' or operator == '>':
            if left:
                if isinstance(curr_elem, dict):
                    curr_elem[key] = left
                elif isinstance(curr_elem, list):
                    curr_elem.append(left)

            if operator == '>':
                curr_elem = parents.pop()

    return root
```
#### 2. Flatten a nested dict:
Sometimes we need to flatten a nested dict in order to iterate all fields at all levels with 1 loop.

```python=
def flatten(dictionary, parent_key=False, separator='.'):
    """
    Turn a nested dictionary into a flattened dictionary
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary
    """

    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, collections.MutableMapping):
            items.extend(flatten(value, new_key, separator).items())
        elif isinstance(value, list):
            for k, v in enumerate(value):
                items.extend(flatten({str(k): v}, new_key).items())
        else:
            items.append((new_key, value))
    return dict(items)
```
#### 3. Nest a flattened dict:
Well, sometimes we also need to nest a dict from a flattened one.

```python=
def make_nested_dict(iterable, final):
    """Make nested dictionary path with a final attribute"""

    # If iterable, keep recursing
    if iterable:

        # Unpack key and rest of dict
        head, *tail = iterable

        # Return new dictionary, recursing on tail value
        return {head: make_nested_dict(tail, final)}

    # Otherwise assign final attribute
    else:
        return final


def merge_nested_dicts(d1, d2):
    """Merge two nested dictionaries together"""

    for key in d2:

        # If we have matching keys
        if key in d1:

            # Only merge if both values are dictionaries
            if isinstance(d2[key], dict) and isinstance(d1[key], dict):
                merge_nested_dicts(d1[key], d2[key])

        # Otherwise assign normally
        else:
            d1[key] = d2[key]

    return d1


def nest_flattened_dict(flat_dict, sep="."):
    nested_dict = {}
    for key in flat_dict:
        # Create path
        paths = key.split(sep)

        # Create nested path
        nested_path = make_nested_dict(paths, flat_dict[key])

        # Update result dict by merging with new dict
        nested_dict = merge_nested_dicts(nested_dict, nested_path)
    return nested_dict
```
#### 4. Get a value(dict, list, object) from a nested field of a dict:
```python=
def get_nested_value_dict(nested_dict: dict, path: list):
    """
    Get a nested value
    :param nested_dict: The nested dictionary
    :param path: The path to the key, ex : "nested_dict.a.b.c" -> path = ["a","b","c"]
    :return: The value 
    """
    if len(path) == 1:
        # here if the path is "a.b.c.0" we will return only if nested.a.b.c is a list
        if isinstance(nested_dict, list) and path[0] == "0":
            return nested_dict  # list
        if isinstance(nested_dict, dict) and path[0] in nested_dict:
            return nested_dict[path[0]]  # dict
    if isinstance(nested_dict, dict) and path and path[0] in nested_dict:
        return get_nested_value_dict(nested_dict[path[0]], path[1:])
    return None
```
#### 5. Set a value for a nested field of a dict:
```python=
def set_nested_value_dict(nested_dict: dict, path: list, value):
    if len(path) == 2 and isinstance(nested_dict, dict) and path[1] == "0" and isinstance(value, list):
        nested_dict[path[0]] = value
    if len(path) == 1:
        if isinstance(nested_dict, list) and isinstance(value, list) and path[0] == "0":
            nested_dict = value
        elif isinstance(nested_dict, dict) and type(value) is type(nested_dict[path[0]]):
            nested_dict[path[0]] = value
    if isinstance(nested_dict, dict) and path and path[0] in nested_dict:
        set_nested_value_dict(nested_dict[path[0]], path[1:], value)
    return nested_dict
```

#### 6. Load a `configuration.yml` file:
```python=
import json
import os

import yaml

__all__ = ["load_config", "load_yaml_config"]


def load_config(config_file='/configuration.yaml'):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    loaded_config = load_yaml_config(dir_path + config_file)
    return loaded_config


def yaml_join(loader, node):
    """Define custom tag handler."""
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])


yaml.add_constructor('!join', yaml_join)


def load_yaml_config(input_file):
    """Input_file is either a text or byte string giving the name.

    (and the path if the file isn't in the current working directory)
    :param input_file: input file
    :return: loaded configuration
    """
    with open(input_file, 'r') as configuration_file:
        loaded_configuration = yaml.load(configuration_file, Loader=yaml.FullLoader)
        return loaded_configuration

```

### Map, Filter and Reduce

#### Multiple filters:
```python=
filters = [lambda x: condtion_function_1(x), lambda x: condtion_function_2(x)]
results = list(filter(lambda x: all([f(x) for f in filters]), target_list))
```

## Apache Airflow
### Script to find failed DAGS:
```python=
def get_latest_errors(hour_last_success):  # noqa: D401
    """Returns a list of list containing the errors' details in the latest time range.

    List format is [ project name, hour of dag failure, failed task,
    url to detailed log, hour of dag failure in local time, dag id].
    """

    bag = DagBag()
    list_dags_ids = list(bag.dags.keys())  # Gets the list of dags ids

    list_latest_dags_on_failure = []

    for dag_id in list_dags_ids:
        dag_runs = DagRun.find(dag_id=dag_id, state='failed')  # Gets the failed dag runs from the corresponding dag_id
        filtered_dag_runs = apply_filter(dag_runs, hour_last_success)
        for dag_run in filtered_dag_runs:

            failed_project = find_project_name_from_dag_id(dag_id)
            hour_of_failure_utc = dag_run.start_date.strftime("%d %b à %Hh%M")

            if len(dag_run.get_task_instances(state='failed')) > 0:
                failed_task_instance = dag_run.get_task_instances(state='failed')[0]

                failed_task = failed_task_instance.task_id
                link_to_detailed_log = failed_task_instance.log_url

            else:
                failed_task = "None"
                link_to_detailed_log = f"{airflow_url}/"

            hour_of_failure_local = convert_datetime_utc_to_local(dag_run.start_date).strftime("%d %b à %Hh%M")
            dag_id = dag_id

            list_latest_dags_on_failure.append(
                [failed_project, hour_of_failure_utc, failed_task, link_to_detailed_log, hour_of_failure_local, dag_id]
            )

    # sorting list_latest_dags_on_failure on x[0] (project name), x[1] (date) and x[1][-5:-3] (hour)
    list_latest_dags_on_failure.sort(key=lambda x: (x[0], x[1], x[1][-5:-3]))

    return list_latest_dags_on_failure

```
## Author

- [@doanhat](https://github.com/doanhat)
- And of course **StackOverflow**

## Links

[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/minhdoan272/)
[![facebook](https://img.shields.io/badge/Facebook-1877F2?style=for-the-badge&logo=facebook&logoColor=white)](https://www.facebook.com/dnminhhhhh/)
[![gmail](https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white)](nhatminhdoan2702@gmail.com)

## License

[![mit](https://img.shields.io/badge/License-MIT-blue.svg)](https://choosealicense.com/licenses/mit/)