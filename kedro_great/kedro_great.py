import datetime
import logging
import os
from copy import copy
from typing import Any, Dict, List, Optional, NamedTuple

import great_expectations as ge
from great_expectations.core.batch import Batch
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.datasource.types import BatchMarkers
from great_expectations.validator.validator import Validator
from great_expectations.exceptions import ConfigNotFoundError
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog, MemoryDataSet

from .exceptions import UnsupportedDataSet, SuiteValidationFailure
from .data import (
    get_suite_names,
    generate_datasource_name,
)


class FailedSuite(NamedTuple):
    suite: str
    dataset: str


class KedroGreat:
    DEFAULT_SUITE_TYPES = ["warning", "basic", None]

    def __init__(
        self,
        expectations_map: Dict = None,
        suite_types: List[Optional[str]] = None,
        run_before_node: bool = True,
        run_after_node: bool = False,
        fail_fast: bool = False,
        fail_after_pipeline_run: bool = False,
    ):
        if expectations_map is None:
            expectations_map = {}
        if suite_types is None:
            suite_types = copy(KedroGreat.DEFAULT_SUITE_TYPES)
        self.expectations_map = expectations_map
        self.suite_types = suite_types

        self._before_node_run = run_before_node
        self._after_node_run = run_after_node
        self._fail_fast = fail_fast
        self._fail_after_pipeline_run = fail_after_pipeline_run

        self.logger = logging.getLogger("KedroGreat")
        self._finished_suites = set()
        self._failed_suites = list()

        try:
            self.expectation_context = ge.data_context.DataContext()
            self.expectation_suite_names = set(
                self.expectation_context.list_expectation_suite_names()
            )
        except ConfigNotFoundError:
            self.logger.error(
                "Great Expectations has not been initialized. "
                "KedroGreat cannot operate. "
                "Please run 'kedro great init'."
            )
            self.expectation_context = None

    @hook_impl
    def after_pipeline_run(self, run_params, pipeline, catalog):
        if self._fail_after_pipeline_run and len(self._failed_suites) > 0:
            raise SuiteValidationFailure(
                f"Failed {len(self._failed_suites)} suites: {self._failed_suites}"
            )

    @hook_impl
    def before_node_run(
        self, catalog: DataCatalog, inputs: Dict[str, Any], run_id: str
    ) -> None:
        if self._before_node_run:
            self._run_validation(catalog, inputs, run_id, read_from_catalog=True)

    @hook_impl
    def after_node_run(
        self, catalog: DataCatalog, outputs: Dict[str, Any], run_id: str
    ) -> None:
        if self._after_node_run:
            self._run_validation(catalog, outputs, run_id, read_from_catalog=False)

    def _run_validation(self, catalog: DataCatalog, data: Dict[str, Any], run_id: str, read_from_catalog: bool):
        if self.expectation_context is None:
            return

        for dataset_name, dataset_value in data.items():
            ran_suite_for_dataset = False

            target_suite_names = get_suite_names(
                self.expectations_map, dataset_name, self.suite_types
            )

            dataset = catalog._get_dataset(dataset_name)
            dataset_path = getattr(dataset, "_filepath", None)
            if read_from_catalog:
                df = dataset_value if isinstance(dataset, MemoryDataSet) else dataset.load()
            else:
                df = dataset_value

            try:
                for target_suite_name in target_suite_names:
                    if (
                        target_suite_name not in self.expectation_suite_names
                        or target_suite_name in self._finished_suites
                    ):
                        continue

                    validation = self._run_suite(
                        dataset_name, dataset_path, df, target_suite_name, run_id
                    )

                    if self._fail_fast and not validation.success:
                        raise SuiteValidationFailure(
                            f"Suite {target_suite_name} for DataSet {dataset_name} failed!"
                        )
                    elif not validation.success:
                        self._failed_suites.append(
                            FailedSuite(target_suite_name, dataset_name)
                        )

                    self._finished_suites.add(target_suite_name)
                    ran_suite_for_dataset = True

                if not ran_suite_for_dataset:
                    self.logger.warning(
                        f"Missing Expectation Suite for DataSet: {dataset_name}"
                    )
            except UnsupportedDataSet:
                self.logger.warning(
                    f"Unsupported DataSet Type: {dataset_name}({type(dataset)})"
                )

    def _run_suite(
        self,
        dataset_name: str,
        dataset_path: Optional[str],
        df: Any,
        target_expectation_suite_name: str,
        run_id: str,
    ):
        target_suite = self.expectation_context.get_expectation_suite(
            target_expectation_suite_name
        )
        batch_markers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )

        batch_kwargs = {"datasource": generate_datasource_name(dataset_name)}

        if dataset_path:
            dataasset_name, _ = os.path.splitext(os.path.basename(dataset_path))
            batch_kwargs["path"] = str(dataset_path)
            batch_kwargs["data_asset_name"] = dataasset_name

        batch = Batch(
            "kedro",
            batch_kwargs=BatchKwargs(batch_kwargs),
            data=df,
            batch_parameters=None,
            batch_markers=batch_markers,
            data_context=self.expectation_context,
        )

        try:
            v = Validator(batch=batch, expectation_suite=target_suite,)
        except ValueError:
            raise UnsupportedDataSet

        validator_dataset_batch = v.get_dataset()
        return self.expectation_context.run_validation_operator(
            "action_list_operator", [validator_dataset_batch], run_id=run_id
        )
