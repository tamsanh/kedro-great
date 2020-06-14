# Kedro Great

As Seen on DataEngineerOne

<p align="center">
  <img width="255" src="https://github.com/tamsanh/kedro-wings/blob/master/images/great.png">
</p>


Kedro Great is an easy-to-use plugin for kedro that makes integration with Great Expectations fast and simple.

Hold yourself accountable to [Great Expectations](https://github.com/great-expectations/great_expectations).  
Never feel afraid of your data changing ever again.  

## Quick Start

### Installation

Kedro Great is available on pypi, and is installed with [kedro hooks](https://kedro.readthedocs.io/en/latest/04_user_guide/15_hooks.html).

#### Command Line

Install the kedro-great pypi package

```console
pip install kedro-great
```

Use the new `kedro great` command to initialize a Great Expectations project and to generate datasources and validations based on the `catalog.yml`
```console
kedro great init
```

#### KedroContext

Add the `KedroGreat` hook to your project context, allowing your pipeline to automatically run validation tests as your pipeline runs.

```python
from kedro_great import KedroGreat


class ProjectContext(KedroContext):
    hooks = (
        KedroGreat(),
    )
```
