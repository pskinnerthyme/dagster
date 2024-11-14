from typing import (
    TYPE_CHECKING,
    Callable,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from dagster import (
    AssetsDefinition,
    AssetSpec,
    SourceAsset,
    _check as check,
)
from dagster._core.definitions.asset_spec import merge_attributes, replace_attributes
from dagster._core.definitions.cacheable_assets import CacheableAssetsDefinition
from dagster._core.definitions.utils import VALID_NAME_REGEX
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.storage.tags import KIND_PREFIX

from dagster_airlift.constants import (
    AIRFLOW_SOURCE_METADATA_KEY_PREFIX,
    DAG_MAPPING_METADATA_KEY,
    PEERED_DAG_MAPPING_METADATA_KEY,
    TASK_MAPPING_METADATA_KEY,
)

if TYPE_CHECKING:
    from dagster_airlift.core.serialization.serialized_data import DagHandle, TaskHandle


def convert_to_valid_dagster_name(name: str) -> str:
    """Converts a name to a valid dagster name by replacing invalid characters with underscores. / is converted to a double underscore."""
    return "".join(c if VALID_NAME_REGEX.match(c) else "__" if c == "/" else "_" for c in name)


def airflow_kind_dict() -> dict:
    return {f"{KIND_PREFIX}airflow": ""}


def airlift_mapped_kind_dict() -> dict:
    return {f"{KIND_PREFIX}airliftmapped": ""}


def spec_iterator(
    assets: Optional[
        Iterable[Union[AssetsDefinition, AssetSpec, SourceAsset, CacheableAssetsDefinition]]
    ],
) -> Iterator[AssetSpec]:
    for asset in assets or []:
        if isinstance(asset, AssetsDefinition):
            yield from asset.specs
        elif isinstance(asset, AssetSpec):
            yield asset
        else:
            raise DagsterInvariantViolationError(
                "Expected orchestrated defs to all be AssetsDefinitions or AssetSpecs."
            )


def metadata_for_task_mapping(*, task_id: str, dag_id: str) -> dict:
    return {TASK_MAPPING_METADATA_KEY: [{"dag_id": dag_id, "task_id": task_id}]}


def metadata_for_dag_mapping(*, dag_id: str) -> dict:
    return {DAG_MAPPING_METADATA_KEY: [{"dag_id": dag_id}]}


def get_metadata_key(instance_name: str) -> str:
    return f"{AIRFLOW_SOURCE_METADATA_KEY_PREFIX}/{instance_name}"


def is_task_mapped_asset_spec(spec: AssetSpec) -> bool:
    return TASK_MAPPING_METADATA_KEY in spec.metadata


def is_dag_mapped_asset_spec(spec: AssetSpec) -> bool:
    return DAG_MAPPING_METADATA_KEY in spec.metadata


def is_peered_dag_asset_spec(spec: AssetSpec) -> bool:
    return PEERED_DAG_MAPPING_METADATA_KEY in spec.metadata


def task_handles_for_spec(spec: AssetSpec) -> Set["TaskHandle"]:
    from dagster_airlift.core.serialization.serialized_data import TaskHandle

    check.param_invariant(is_task_mapped_asset_spec(spec), "spec", "Must be mapped spec")
    task_handles = []
    for task_handle_dict in spec.metadata[TASK_MAPPING_METADATA_KEY]:
        task_handles.append(
            TaskHandle(dag_id=task_handle_dict["dag_id"], task_id=task_handle_dict["task_id"])
        )
    return set(task_handles)


def dag_handles_for_spec(spec: AssetSpec) -> Set["DagHandle"]:
    from dagster_airlift.core.serialization.serialized_data import DagHandle

    check.param_invariant(is_dag_mapped_asset_spec(spec), "spec", "Must be mapped spec")
    return {
        DagHandle(dag_id=dag_handle_dict["dag_id"])
        for dag_handle_dict in spec.metadata[DAG_MAPPING_METADATA_KEY]
    }


def peered_dag_handles_for_spec(spec: AssetSpec) -> Set["DagHandle"]:
    from dagster_airlift.core.serialization.serialized_data import DagHandle

    check.param_invariant(is_peered_dag_asset_spec(spec), "spec", "Must be mapped spec")
    return {
        DagHandle(dag_id=dag_handle_dict["dag_id"])
        for dag_handle_dict in spec.metadata[PEERED_DAG_MAPPING_METADATA_KEY]
    }


AssetSpecPredicate = Callable[[AssetSpec], bool]


class AssetSpecSequence(Sequence[AssetSpec]):
    def __init__(self, asset_specs: Sequence[AssetSpec], can_transform: bool = True):
        self._asset_specs = asset_specs
        self._can_transform = can_transform

    def __getitem__(self, item: int) -> AssetSpec:
        return self._asset_specs[item]

    def __len__(self) -> int:
        return len(self._asset_specs)

    def __iter__(self) -> Iterator[AssetSpec]:
        return iter(self._asset_specs)

    def split(
        self, include: Callable[[AssetSpec], bool]
    ) -> Tuple["AssetSpecSequence", "AssetSpecSequence"]:
        return AssetSpecSequence(
            [asset_spec for asset_spec in self if include(asset_spec)], can_transform=False
        ), AssetSpecSequence(
            [asset_spec for asset_spec in self if not include(asset_spec)], can_transform=False
        )

    def filter(self, where: AssetSpecPredicate) -> "AssetSpecSequence":
        return AssetSpecSequence(
            [asset_spec for asset_spec in self if where(asset_spec)], can_transform=False
        )

    def replace_attributes(
        self, attrs: dict, where: Callable[[AssetSpec], bool]
    ) -> "AssetSpecSequence":
        if not self._can_transform:
            raise Exception("Cannot transform this sequence")
        return AssetSpecSequence(
            [
                replace_attributes(asset_spec, **attrs) if where(asset_spec) else asset_spec
                for asset_spec in self
            ],
        )

    def merge_attributes(
        self, attrs: dict, where: Callable[[AssetSpec], bool]
    ) -> "AssetSpecSequence":
        # We only want to perform operations on the full set of asset specs. This removes the footgun where
        # there are multiple sets of assets floating around, and we perform a transformation on a subset, but
        # then provide the wrong subset to Definitions.
        # The idea would be that we always provide the full set of assets to Definitions
        if not self._can_transform:
            raise Exception("Cannot transform this sequence")
        return AssetSpecSequence(
            [
                merge_attributes(asset_spec, **attrs) if where(asset_spec) else asset_spec
                for asset_spec in self
            ],
        )
