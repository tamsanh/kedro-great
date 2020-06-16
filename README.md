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
