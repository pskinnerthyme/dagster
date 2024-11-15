from dagster import (
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    multi_asset,
)
from dagster._core.definitions.asset_spec import replace_attributes
from dagster._core.definitions.declarative_automation.automation_condition import (
    AutomationCondition,
)
from dagster_airlift.core import (
    AirflowBasicAuthBackend,
    AirflowInstance,
    build_airflow_polling_sensor,
    load_airflow_dag_asset_specs,
)

upstream_airflow_instance = AirflowInstance(
    auth_backend=AirflowBasicAuthBackend(
        webserver_url="http://localhost:8081",
        username="admin",
        password="admin",
    ),
    name="upstream",
)

downstream_airflow_instance = AirflowInstance(
    auth_backend=AirflowBasicAuthBackend(
        webserver_url="http://localhost:8082",
        username="admin",
        password="admin",
    ),
    name="downstream",
)

load_customers_dag_asset = next(
    iter(
        load_airflow_dag_asset_specs(
            airflow_instance=upstream_airflow_instance,
            dag_selector_fn=lambda dag: dag.dag_id == "load_customers",
        )
    )
)
customer_metrics_dag_asset = replace_attributes(
    next(
        iter(
            load_airflow_dag_asset_specs(
                airflow_instance=downstream_airflow_instance,
                dag_selector_fn=lambda dag: dag.dag_id == "customer_metrics",
            )
        )
        # Add a dependency on the load_customers_dag_asset
    ),
    deps=[load_customers_dag_asset],
    automation_condition=AutomationCondition.eager(),
)


@multi_asset(specs=[customer_metrics_dag_asset])
def run_customer_metrics() -> MaterializeResult:
    run_id = downstream_airflow_instance.trigger_dag("customer_metrics")
    downstream_airflow_instance.wait_for_run_completion("customer_metrics", run_id)
    if downstream_airflow_instance.get_run_state("customer_metrics", run_id) == "success":
        return MaterializeResult(asset_key=customer_metrics_dag_asset.key)
    else:
        raise Exception("Dag run failed.")


upstream_sensor = build_airflow_polling_sensor(
    mapped_assets=[load_customers_dag_asset],
    airflow_instance=upstream_airflow_instance,
)
downstream_sensor = build_airflow_polling_sensor(
    mapped_assets=[customer_metrics_dag_asset],
    airflow_instance=downstream_airflow_instance,
)

automation_sensor = AutomationConditionSensorDefinition(
    name="automation_sensor",
    target="*",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=1,
)

defs = Definitions(
    assets=[load_customers_dag_asset, run_customer_metrics],
    sensors=[upstream_sensor, downstream_sensor],
)
