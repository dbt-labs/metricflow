import datetime
import logging
import os
from pathlib import Path

import pandas as pd
import pytest

from metricflow.dataflow.sql_table import SqlTable
from metricflow.object_utils import pformat_big_objects
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql_clients.sql_utils import make_df
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.table_snapshot.table_snapshots import (
    SqlTableSnapshotRepository,
    SqlTableSnapshotRestorer,
)
from metricflow.time.time_constants import ISO8601_PYTHON_FORMAT
from metricflow.time.time_granularity import period_begin_offset
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)

DEFAULT_DS = "ds"


def create_table(sql_client: SqlClient, sql_table: SqlTable, df: pd.DataFrame) -> None:  # noqa: D
    # TODO: Replace with table_exists() check.
    sql_client.drop_table(sql_table)

    sql_client.create_table_from_dataframe(
        sql_table=sql_table,
        df=df,
    )


@pytest.fixture(scope="session")
def source_table_snapshot_repository() -> SqlTableSnapshotRepository:  # noqa: D
    return SqlTableSnapshotRepository(Path(os.path.dirname(__file__)).joinpath("source_table_snapshots"))


@pytest.fixture(scope="session")
def create_source_tables(
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    source_table_snapshot_repository: SqlTableSnapshotRepository,
) -> None:
    """Creates all tables that should be in the source schema.

    If a table with a given name already exists in the source schema, it's assumed to have the expected schema / data.
    """
    schema_name = mf_test_session_state.mf_source_schema
    # Figure out which tables are missing from the source schema.
    expected_table_names = sorted(
        [table_snapshot.table_name for table_snapshot in source_table_snapshot_repository.table_snapshots]
    )
    logger.info(
        f"The following tables are needed in schema {schema_name}:\n" f"{pformat_big_objects(expected_table_names)}"
    )
    source_schema_table_names = sorted(sql_client.list_tables(schema_name=schema_name))

    missing_table_names = set(expected_table_names).difference(source_schema_table_names)
    logger.info(
        f"The following tables are missing and will be restored:\n"
        f"{pformat_big_objects(sorted(missing_table_names))}"
    )
    # Restore the ones that are missing.
    snapshot_restorer = SqlTableSnapshotRestorer(
        sql_client=sql_client, schema_name=mf_test_session_state.mf_source_schema
    )
    for table_snapshot in source_table_snapshot_repository.table_snapshots:
        if table_snapshot.table_name in missing_table_names:
            logger.info(f"Restoring: {table_snapshot.table_name}")
            snapshot_restorer.restore(table_snapshot)


@pytest.fixture(scope="session")
def create_simple_model_tables(create_source_tables: None) -> bool:
    """Creates tables with example source data for testing the simple model.

    The use of this fixture will be consolidated with the create_source_tables fixture in a later PR once the other
    table creation fixtures are migrated.
    """
    pass


@pytest.fixture(scope="session")
def create_scd_model_tables(create_simple_model_tables: bool) -> bool:
    """This fixture ensures the scd tables are created when necessary

    The SCD model relies on the simple model tables, so we simply depend on that fixture and return it here.
    If, at any point, we decide to diverge from the simple model we can update this fixture independently.
    """
    return create_simple_model_tables


@pytest.fixture(scope="session")
def create_bridge_table(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> bool:
    """Creates tables with example source data for multi-hop join testing."""
    schema = mf_test_session_state.mf_source_schema

    # NOTE: fct_bookings should have both a host and guest id, but we do not yet
    # have "entity roles" so that a user dimension can be attached to either
    # guest or host (e.g. bookings by user:guest/country)
    customer_accounts = [
        # ["customer_id", "customer_name", "customer_atomic_weight", "ds_partitioned"]
        ("0", "thorium", "90", "2020-01-01"),
        ("1", "osmium", "76", "2020-01-02"),
        ("2", "tellurium", "52", "2020-01-01"),
        ("3", "einsteinium", "99", "2020-01-04"),
    ]
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="customer_table"),
        df=make_df(
            sql_client=sql_client,
            columns=["customer_id", "customer_name", "customer_atomic_weight", "ds_partitioned"],
            time_columns={"ds_partitioned"},
            data=customer_accounts,
        ),
    )

    customer_other_data = [
        # ["customer_id", "country", "customer_third_hop_id"]
        ("0", "turkmenistan", "another_id0"),
        ("1", "paraguay", "another_id1"),
        ("2", "myanmar", "another_id2"),
        ("3", "djibouti", "another_id3"),
    ]
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="customer_other_data"),
        df=make_df(
            sql_client=sql_client,
            columns=["customer_id", "country", "customer_third_hop_id"],
            data=customer_other_data,
        ),
    )

    third_hop_table = [
        # ["customer_third_hop_id", "value"]
        ("another_id0", "citadel"),
        ("another_id1", "virtu"),
        ("another_id2", "two sigma"),
        ("another_id3", "jump"),
    ]
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="third_hop_table"),
        df=make_df(
            sql_client=sql_client,
            columns=["customer_third_hop_id", "value"],
            data=third_hop_table,
        ),
    )

    bridge_data = [
        # ["account_id", "customer_id", "extra_dim", "ds_partitioned"]
        ("a0", "0", "lux", "2020-01-01"),
        ("a0", "1", "lux", "2020-01-02"),
        ("a1", "2", "not_lux", "2020-01-01"),
        ("a2", "3", "super_lux", "2020-01-04"),
    ]
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="bridge_table"),
        df=make_df(
            sql_client=sql_client,
            columns=["account_id", "customer_id", "extra_dim", "ds_partitioned"],
            time_columns={"ds_partitioned"},
            data=bridge_data,
        ),
    )

    account_month_txns = [
        # While datetime columns can be used here, somewhere there is a conversion to a string when testing
        # with sqlite, so when you read the datetime column, it comes out as a string.
        # Columns for reference
        # ["account_id", DEFAULT_DS, "account_month", "txn_count", "txns_value", "ds_partitioned"]
        ("a0", "2020-01-01", "JANUARY", 3, 300, "2020-01-01"),
        ("a0", "2020-02-01", "FEBRUARY", 4, 400, "2020-02-01"),
        ("a0", "2020-03-01", "MARCH", 5, 1000, "2020-03-01"),
        ("a0", "2020-04-01", "APRIL", 4, 600, "2020-04-01"),
        ("a1", "2020-01-01", "JANUARY", 2, 200, "2020-01-01"),
        ("a1", "2020-02-01", "FEBRUARY", 5, 250, "2020-02-01"),
        ("a1", "2020-03-01", "MARCH", 6, 360, "2020-03-01"),
        ("a1", "2020-04-01", "APRIL", 10, 1000, "2020-04-01"),
        ("a2", "2020-01-01", "JANUARY", 5, 500, "2020-01-01"),
    ]
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="account_month_txns"),
        df=make_df(
            sql_client=sql_client,
            columns=["account_id", DEFAULT_DS, "account_month", "txn_count", "txns_value", "ds_partitioned"],
            time_columns={DEFAULT_DS, "ds_partitioned"},
            data=account_month_txns,
        ),
    )
    return True


@pytest.fixture(scope="session")
def create_message_source_tables(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> bool:
    """Creates tables with example source data for testing."""
    schema = mf_test_session_state.mf_source_schema

    data = [
        ("t0", "u0", "m0", "2020-01-01"),
        ("t0", "u0", "m1", "2020-01-01"),
        ("t0", "u1", "m2", "2020-01-01"),
        ("t0", "u1", "m3", "2020-01-01"),
        ("t0", "u1", "m4", "2020-01-01"),
        ("t1", "u0", "m5", "2020-01-02"),
        ("t1", "u0", "m7", "2020-01-02"),
        ("t1", "u0", "m8", "2020-01-02"),
        ("t1", "u1", "m9", "2020-01-02"),
        ("t1", "u1", "m10", "2020-01-02"),
        ("t1", "u1", "m11", "2020-01-02"),
        ("t1", "u1", "m12", "2020-01-03"),
        ("t1", "u2", "m13", "2020-01-03"),
        ("t1", "u3", "m14", "2020-01-03"),
    ]
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="fct_messages"),
        df=make_df(
            sql_client=sql_client,
            columns=["team_id", "user_id", "message_id", DEFAULT_DS],
            time_columns={DEFAULT_DS},
            data=data,
        ),
    )
    user_data = [
        ("t0", "u0", "burkina faso", "2020-01-01", "u0_2"),
        ("t0", "u1", "vanuatu", "2020-01-01", "u1_2"),
        ("t1", "u0", "merica", "2020-01-01", "u0_2"),
        ("t1", "u1", "merica", "2020-01-01", "u1_2"),
        ("t1", "u2", "burkina faso", "2020-01-01", "u2_2"),
        ("t1", "u3", "vanuata", "2020-01-01", "u3_2"),
    ]
    # Like fct_bookings, but using dt instead of ds to verify that a primary time dimension named something other than
    # ds can be used.
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="fct_users"),
        df=make_df(
            sql_client=sql_client,
            columns=["team_id", "id", "country", "created_at", "ident_2"],
            time_columns={"created_at"},
            data=user_data,
        ),
    )

    users_more_data = [
        ("u0", "thorium", "u0_2"),
        ("u1", "tellurium", "u1_2"),
        ("u0", "osmium", "u0_2"),
        ("u1", "tecnetium", "u1_2"),
        ("u2", "einsteinium", "u2_2"),
        ("u3", "rubidium", "u3_2"),
    ]
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="fct_users_more"),
        df=pd.DataFrame(
            columns=["id", "user_element", "ident_2"],
            data=users_more_data,
        ),
    )
    return True


@pytest.fixture(scope="session")
def create_extended_date_model_tables(mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient) -> bool:
    """Creates fct_bookings_extended / dim_listings_extended with a year's worth of dates for granularity testing."""
    schema = mf_test_session_state.mf_source_schema

    # Create fct_bookings_extended that has rows for every date in 2020.
    # Also create dim_listings_extended to match.
    start_date = datetime.datetime(2020, 1, 1)
    end_date = datetime.datetime(2020, 12, 31)

    current_date = start_date
    fct_bookings_extended_data = []
    dim_listings_extended_data = []
    booking_id = 0
    listing_id = 0
    while current_date <= end_date:
        ds_as_str = current_date.strftime(ISO8601_PYTHON_FORMAT)
        fct_bookings_extended_data.append((booking_id, 1, listing_id, booking_id % 2 == 0, ds_as_str))
        dim_listings_extended_data.append((listing_id, ds_as_str))

        listing_id += 1
        # Have 10 different listing IDs so that we get different distinct values when we set the granularity
        # to daily / weekly / monthly
        listing_id = listing_id % 10
        current_date = current_date + datetime.timedelta(days=1)

        booking_id += 1

    sql_table = SqlTable(schema_name=schema, table_name="fct_bookings_extended")
    sql_client.drop_table(sql_table)

    sql_client.create_table_from_dataframe(
        sql_table=sql_table,
        df=make_df(
            sql_client=sql_client,
            columns=["booking_id", "booking", "listing_id", "is_instant", DEFAULT_DS],
            time_columns={DEFAULT_DS},
            data=fct_bookings_extended_data,
        ),
    )

    sql_table = SqlTable(schema_name=schema, table_name="dim_listings_extended")
    sql_client.drop_table(sql_table)

    sql_client.create_table_from_dataframe(
        sql_table=sql_table,
        df=make_df(
            sql_client=sql_client,
            columns=["listing_id", "listing_creation_ds"],
            time_columns={"listing_creation_ds"},
            data=dim_listings_extended_data,
        ),
    )

    # Create fct_bookings_extended_monthly with data at a monthly granularity.
    # Example rows:
    # ["bookings_monthly", "is_instant", "ds"]
    # 10                   True          2020-01-01
    # 5                    False         2020-01-01
    # 10                   True          2020-02-01
    # 5                    False         2020-02-01
    current_date = pd.Timestamp(start_date)
    fct_bookings_extended_monthly_data = []
    while current_date <= end_date:
        fct_bookings_extended_monthly_data.append((10, True, current_date.strftime(ISO8601_PYTHON_FORMAT)))
        fct_bookings_extended_monthly_data.append((5, False, current_date.strftime(ISO8601_PYTHON_FORMAT)))

        current_date += period_begin_offset(TimeGranularity.MONTH)

    sql_table = SqlTable(schema_name=schema, table_name="fct_bookings_extended_monthly")
    sql_client.drop_table(sql_table=sql_table)
    sql_client.create_table_from_dataframe(
        sql_table=sql_table,
        df=make_df(
            sql_client=sql_client,
            columns=["bookings_monthly", "is_instant", DEFAULT_DS],
            time_columns={DEFAULT_DS},
            data=fct_bookings_extended_monthly_data,
        ),
    )

    return True


@pytest.fixture(scope="session")
def create_data_warehouse_validation_model_tables(
    mf_test_session_state: MetricFlowTestSessionState, sql_client: SqlClient
) -> bool:
    """Creates tables with example source data for testing."""
    schema = mf_test_session_state.mf_source_schema

    data = [
        (True, "2022-06-01"),
    ]
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="fct_animals"),
        df=make_df(
            sql_client=sql_client,
            columns=["is_dog", DEFAULT_DS],
            time_columns={DEFAULT_DS},
            data=data,
        ),
    )

    return True
