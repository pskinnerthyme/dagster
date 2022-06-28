import pytest

from dagster import (
    AssetKey,
    Out,
    ResourceDefinition,
    graph,
    io_manager,
    op,
    with_resources,
    without_resources,
)
from dagster.core.asset_defs import AssetsDefinition, SourceAsset, asset, build_assets_job
from dagster.core.asset_defs.assets import io_manager_key_for_asset_key
from dagster.core.definitions.utils import DEFAULT_IO_MANAGER_KEY
from dagster.core.errors import DagsterInvalidDefinitionError


def test_asset():
    @asset(
        resource_defs={
            "foo": ResourceDefinition.hardcoded_resource("foo"),
            "bar": ResourceDefinition.hardcoded_resource("bar"),
        },
        required_resource_keys={"baz"},
    )
    def the_asset():
        pass

    transformed_def = without_resources([the_asset])[0]
    assert transformed_def.required_resource_keys == the_asset.required_resource_keys
    assert transformed_def.resource_defs == {}

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match="resource with key 'bar' required by op 'the_asset' was not provided.",
    ):
        with_resources([transformed_def], {})


def test_asset_defines_io_manager():
    @io_manager
    def the_io_manager():
        pass

    @asset(io_manager_def=the_io_manager)
    def the_asset():
        return 5

    assert the_asset.required_resource_keys == {io_manager_key_for_asset_key(the_asset.asset_key)}
    transformed_def = without_resources([the_asset])[0]
    assert transformed_def.required_resource_keys == {DEFAULT_IO_MANAGER_KEY}

    result = build_assets_job("test", [transformed_def]).execute_in_process()
    assert result.success
    assert result.output_for_node("the_asset") == 5


def test_without_resources_graph_backed_asset():
    @op(required_resource_keys={"foo"})
    def the_op(context):
        return context.resources.foo

    @graph
    def the_graph():
        return the_op()

    graph_backed = AssetsDefinition(
        keys_by_input_name={},
        keys_by_output_name={"result": AssetKey("cool_thing")},
        node_def=the_graph,
        resource_defs={"foo": ResourceDefinition.hardcoded_resource("bar")},
    )

    assert graph_backed.required_resource_keys == {"foo", DEFAULT_IO_MANAGER_KEY}

    transformed_def = without_resources([graph_backed])[0]

    assert transformed_def.required_resource_keys == {"foo", DEFAULT_IO_MANAGER_KEY}
    assert transformed_def.resource_defs == {}

    transformed_again = with_resources(
        [transformed_def], {"foo": ResourceDefinition.hardcoded_resource("baz")}
    )[0]

    result = build_assets_job("test", [transformed_again]).execute_in_process()
    assert result.success

    assert result.output_for_node("the_graph") == "baz"


def test_asset_defines_io_manager_key():
    @io_manager
    def the_manager():
        pass

    @asset(io_manager_key="the_key", resource_defs={"the_key": the_manager})
    def the_asset():
        pass

    transformed_asset = without_resources([the_asset])[0]
    assert transformed_asset.required_resource_keys == {"the_key"}


def test_source_asset():
    foo_resource = ResourceDefinition.hardcoded_resource("foo")

    @io_manager(required_resource_keys={"foo"})
    def the_io_manager():
        pass

    the_asset = SourceAsset(
        key=AssetKey("the_asset"),
        io_manager_def=the_io_manager,
        resource_defs={"foo": foo_resource},
    )
    assert the_asset.resource_defs == {
        io_manager_key_for_asset_key(AssetKey("the_asset")): the_io_manager,
        "foo": foo_resource,
    }
    transformed_asset = without_resources([the_asset])[0]
    assert transformed_asset.resource_defs == {}
    assert transformed_asset.get_io_manager_key() == DEFAULT_IO_MANAGER_KEY


def test_source_asset_defines_io_manager_key():
    @io_manager
    def the_manager():
        pass

    the_asset = SourceAsset(
        key=AssetKey("the_asset"), io_manager_key="the_key", resource_defs={"the_key": the_manager}
    )
    assert the_asset.get_io_manager_key() == "the_key"
    transformed_asset = without_resources([the_asset])[0]
    assert transformed_asset.get_io_manager_key() == "the_key"
    assert transformed_asset.resource_defs == {}


def test_io_manager_key_overrides_multiple_outputs():
    # Simulate setting system-provided io manager key for multiple outs. Once https://github.com/dagster-io/dagster/issues/8650 lands, this should be replaced with using @multi_asset with AssetOut.

    asset_key_a = AssetKey("a")
    asset_key_b = AssetKey("b")

    @op(
        out={
            "a": Out(io_manager_key=io_manager_key_for_asset_key(asset_key_a)),
            "b": Out(io_manager_key=io_manager_key_for_asset_key(asset_key_b)),
        }
    )
    def ma_op():
        return 1

    ma = AssetsDefinition(
        node_def=ma_op,
        keys_by_input_name={},
        keys_by_output_name={"a": asset_key_a, "b": asset_key_b},
    )

    assert ma.required_resource_keys == {
        io_manager_key_for_asset_key(asset_key_a),
        io_manager_key_for_asset_key(asset_key_b),
    }

    ma_transformed = without_resources([ma])[0]
    assert ma_transformed.required_resource_keys == {DEFAULT_IO_MANAGER_KEY}


def test_without_resources_transitive_io_manager_deps():
    @io_manager(required_resource_keys={"foo"})
    def the_io_manager():
        pass

    @asset(
        resource_defs={"foo": ResourceDefinition.hardcoded_resource("foo")},
        io_manager_def=the_io_manager,
    )
    def the_asset():
        pass

    assert the_asset.required_resource_keys == {
        "foo",
        io_manager_key_for_asset_key(the_asset.asset_key),
    }
    transformed_asset = without_resources([the_asset])[0]
    # Transitive resource dependencies are kept around, because there are is no way to specify resources as optional.
    assert transformed_asset.required_resource_keys == {"foo", DEFAULT_IO_MANAGER_KEY}
