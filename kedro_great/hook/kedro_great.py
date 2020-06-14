import datetime
import logging
from typing import Any, Dict, List

import great_expectations as ge
from great_expectations.core.batch import Batch
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.datasource.types import BatchMarkers
from great_expectations.validator.validator import Validator
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog, AbstractDataSet

from ..data import (
    identify_dataset_type,
    get_suite_name,
    get_ge_class,
    generate_datasource_name,
)


class KedroGreat:
    DEFAULT_SUITE_TYPES = ["warning", "basic", None]

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
            suite_types = []
        suite_types += KedroGreat.DEFAULT_SUITE_TYPES
        self.expectations_map = expectations_map
        self.suite_types = suite_types

        self.expectation_context = ge.data_context.DataContext()
        self.expectation_suite_names = set(
            self.expectation_context.list_expectation_suite_names()
        )
        self._before_node_run = run_before_node
        self._after_node_run = run_after_node
        self.logger = logging.getLogger("KedroGreat")
        self._finished_suites = set()

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

    def _run_validation(self, catalog: DataCatalog, data: Dict[str, Any], run_id: str):
        for dataset_name, dataset_value in data.items():
            ran_suite_for_dataset = False
            for suite_type in self.suite_types:

                target_suite_name = get_suite_name(
                    self.expectations_map, dataset_name, suite_type
                )
                if (
                    target_suite_name not in self.expectation_suite_names
                    or target_suite_name in self._finished_suites
                ):
                    continue

                dataset = catalog._get_dataset(dataset_name)
                dataset_type = identify_dataset_type(dataset)
                dataset_class = get_ge_class(dataset_type)
                if dataset_class is None:
                    self.logger.warning(
                        f"Unsupported DataSet Type: {dataset_name}({type(dataset)})"
                    )
                    continue

                self._run_suite(dataset_name, dataset, target_suite_name, run_id)
                self._finished_suites.add(target_suite_name)
                ran_suite_for_dataset = True

            if not ran_suite_for_dataset:
                self.logger.warning(
                    f"Missing Expectation Suite for DataSet: {dataset_name}"
                )

    def _run_suite(
        self,
        dataset_name: str,
        dataset: AbstractDataSet,
        target_expectation_suite_name: str,
        run_id: str,
    ):
        target_suite = self.expectation_context.get_expectation_suite(
            target_expectation_suite_name
        )
        dataset_path = str(dataset._filepath)
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
            batch_kwargs=BatchKwargs(
                {
                    "path": dataset_path,
                    "datasource": generate_datasource_name(dataset_name),
                }
            ),
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
                "class_name": identify_dataset_type(dataset),
            },
        )
        validator_dataset_batch = v.get_dataset()
        return self.expectation_context.run_validation_operator(
            "action_list_operator", [validator_dataset_batch], run_id=run_id
        )
