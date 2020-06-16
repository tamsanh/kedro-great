from typing import Dict, Optional, List, Type, Union

from great_expectations.cli.datasource import DatasourceTypes
from kedro.io import AbstractDataSet


def identify_dataset_type(
    dataset: AbstractDataSet,
    pandas_datasets: Optional[List[Type[AbstractDataSet]]] = None,
    spark_datasets: Optional[List[Type[AbstractDataSet]]] = None,
) -> Optional[DatasourceTypes]:
    from kedro.extras.datasets.spark import SparkDataSet
    from kedro.extras.datasets.pandas import CSVDataSet, ExcelDataSet

    default_pandas_datasets = [
        CSVDataSet,
        ExcelDataSet,
    ]

    default_spark_datasets = [SparkDataSet]

    if pandas_datasets is None:
        pandas_datasets = []
    if spark_datasets is None:
        spark_datasets = []

    pandas_datasets += default_pandas_datasets
    spark_datasets += default_spark_datasets

    def _dataset_isinstance_of_list(
        target_dataset: AbstractDataSet, dataset_list: List[Type[AbstractDataSet]]
    ) -> bool:
        return any(
            [isinstance(target_dataset, dataset_type) for dataset_type in dataset_list]
        )

    if _dataset_isinstance_of_list(dataset, pandas_datasets):
        return DatasourceTypes.PANDAS
    elif _dataset_isinstance_of_list(dataset, spark_datasets):
        return DatasourceTypes.SPARK
    else:
        return None


def generate_datasource_name(dataset_name: str) -> str:
    return f"{dataset_name}___kedro_great_datasource"


def get_suite_names(
    expectations_map: Dict[str, Union[str, List[str]]],
    dataset_name: str,
    suite_types: List[str],
) -> List[str]:
    found_mappings = expectations_map.get(dataset_name, dataset_name)
    if type(found_mappings) is str:
        found_mappings = [found_mappings]

    target_suite_names = []

    for found_mapping in found_mappings:
        for suite_type in suite_types:
            if "." in found_mapping or suite_type is None:
                target_expectation_suite_name = f"{found_mapping}"
            else:
                target_expectation_suite_name = f"{found_mapping}.{suite_type}"
            target_suite_names.append(target_expectation_suite_name)

    return list(set(target_suite_names))
