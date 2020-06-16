# Kedro Great

As Seen on DataEngineerOne

<p align="center">
  <img width="255" src="https://github.com/tamsanh/kedro-great/blob/master/images/great.png">
</p>


Kedro Great is an easy-to-use plugin for kedro that makes integration with Great Expectations fast and simple.

Hold yourself accountable to [Great Expectations](https://github.com/great-expectations/great_expectations).  
Never have fear of data silently changing ever again.

## Quick Start

#### Install
Kedro Great is available on pypi, and is installed with [kedro hooks](https://kedro.readthedocs.io/en/latest/04_user_guide/15_hooks.html).

```console
pip install kedro-great
```

#### Setup

Once installed, `kedro great` becomes available as a kedro command.

You can use `kedro great init` to initialize a Great Expectations project, and then automatically generate its project context.

Furthermore, by using `kedro great init`, you also generate Great Expectations `Datasource`s and `Suite`s to use with your `catalog.yml` DataSets.

By default, expectation suites are named for the `catalog.yml` name and a `basic.json` is generated for each.

```console
kedro great init
```

#### Use

After the Great Expectations project has been setup and configured, you can now use the `KedroGreat` hook to run all your data validations every time the pipeline runs.

```python
# run.py
from kedro_great import KedroGreat

class ProjectContext(KedroContext):
    hooks = (
        KedroGreat(),
    )
```


Then just run the kedro pipeline to run the suites.

```console
kedro run
```

#### Results

Finally, you can use `great_expectations` itself to generate documentation and view the results of your pipeline.

Love seeing those green ticks!

```console
great_expectations docs build
```

## Hook Options

The `KedroGreat` hook supports a few options currently. If you wish to 

### expectations_map: Dict[str, Union[str, List[str]]]

If you have multiple expectation suites you wish to run, or expectation suites that do not have the same name
as the catalog dataset, these mappings can be specified in the `expectations_map` argument for `KedroGreat`

**Default:** The catalog name is the expectation name.

*Note:* Specifying a suite type such as `.basic` will override all other suite types

```python
KedroGreat(expectations_map={
    'pandas_iris_data': 'pandas_iris_data',
    'spark_iris_data': ['spark_iris_data',
                        'other_expectation',
                        'another_expectation.basic'],

})
```

### suite_types: List[Optional[str]]

If your suites have multiple types, you can choose exactly which types to run.

A `None` means that a suite will not have the type appended to the name.

**Default:** The `KedroGreat.DEFAULT_SUITE_TYPES`.

*Node:* If a suite type is already specified in the `expectations_map`, that will override this list.

```python
KedroGreat(suite_types=[
    'warning',
    'basic',
    None
])
```

### run_before_node:bool, run_after_node: bool

You can decide when the suites run, before or after a node or both before and after a node.

It will operate on the node `inputs` and `outputs` respectively.

**Default:** Only runs before a node runs.

```python
KedroGreat(run_before_node=True, run_after_node=False)
```
