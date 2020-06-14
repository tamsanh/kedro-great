import datetime
from typing import Any, Dict, List, Optional

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
import great_expectations as ge
from great_expectations.core.batch import Batch
from great_expectations.datasource.types import BatchMarkers
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.validator.validator import Validator

import logging


class KedroGreat:
    def __init__(
        self,
        expectations_map: Dict = None,
        suite_types: List[str] = None,
        run_before_node: bool = True,
        run_after_node: bool = False,
    ):
        if expectations_map is None:
            expectations_map = {}
        if suite_types is None:
            suite_types = ["warning", None]
        self.expectations_map = expectations_map
        self.suite_types = suite_types

        self.expectation_context = ge.data_context.DataContext()
        self.expectation_suite_names = set(
            self.expectation_context.list_expectation_suite_names()
        )
        self._before_node_run = run_before_node
        self._after_node_run = run_after_node
        self.logger = logging.getLogger("KedroGreat")

    @hook_impl
    def before_node_run(
        self, catalog: DataCatalog, inputs: Dict[str, Any], run_id: str
    ) -> None:
        if self._before_node_run:
            self._run_validation(catalog, inputs, run_id)

    @hook_impl
    def after_node_run(
        self, catalog: DataCatalog, outputs: Dict[str, Any], run_id: str
    ) -> None:
        if self._after_node_run:
            self._run_validation(catalog, outputs, run_id)

    def _get_suite_name(self, dataset_name: str, suite_type: Optional[str]) -> str:
        if suite_type is None:
            target_expectation_suite_name = (
                f"{self.expectations_map.get(dataset_name, dataset_name)}"
            )
        else:
            target_expectation_suite_name = (
                f"{self.expectations_map.get(dataset_name, dataset_name)}.{suite_type}"
            )
        return target_expectation_suite_name

    def _run_validation(self, catalog: DataCatalog, data: Dict[str, Any], run_id: str):
        for dataset_name, dataset_value in data.items():
            ran_suite_for_dataset = False
            for suite_type in self.suite_types:

                target_suite_name = self._get_suite_name(dataset_name, suite_type)
                if target_suite_name not in self.expectation_suite_names:
                    continue

                dataset = catalog._get_dataset(dataset_name)
                dataset_class = self._get_ge_class_name(dataset)
                if dataset_class is None:
                    self.logger.warning(
                        f"Unsupported DataSet Type: {dataset_name}({type(dataset)})"
                    )
                    continue

                self._run_suite(dataset, target_suite_name, run_id)
                ran_suite_for_dataset = True

            if not ran_suite_for_dataset:
                self.logger.warning(
                    f"Missing Expectation Suite for DataSet: {dataset_name}"
                )

    @staticmethod
    def _get_ge_class_name(dataset):
        from kedro.extras.datasets.spark import SparkDataSet
        from kedro.extras.datasets.pandas import CSVDataSet

        if isinstance(dataset, CSVDataSet):
            return "PandasDataset"
        elif isinstance(dataset, SparkDataSet):
            return "SparkDFDataset"
        else:
            return None

    def _run_suite(self, dataset, target_expectation_suite_name, run_id):
        target_suite = self.expectation_context.get_expectation_suite(
            target_expectation_suite_name
        )
        df = dataset.load()
        batch_markers = (
            BatchMarkers(
                {
                    "ge_load_time": datetime.datetime.now(
                        datetime.timezone.utc
                    ).strftime("%Y%m%dT%H%M%S.%fZ")
                }
            ),
        )
        batch = Batch(
            "kedro",
            batch_kwargs=BatchKwargs({"path": "kedro", "datasource": "kedro"}),
            data=df,
            batch_parameters=None,
            batch_markers=batch_markers,
            data_context=self.expectation_context,
        )
        v = Validator(
            batch=batch,
            expectation_suite=target_suite,
            expectation_engine={
                "module_name": "great_expectations.dataset",
                "class_name": self._get_ge_class_name(dataset),
            },
        )
        validator_dataset_batch = v.get_dataset()
        return self.expectation_context.run_validation_operator(
            "action_list_operator", [validator_dataset_batch], run_id=run_id
        )
