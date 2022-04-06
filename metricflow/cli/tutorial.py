import pandas as pd
import os
from string import Template
from typing import Dict

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql_clients.sql_utils import make_df


COUNTRIES = [("US", "NA"), ("MX", "NA"), ("CA", "NA"), ("BR", "SA"), ("GR", "EU"), ("FR", "EU")]
TRANSACTION_TYPE = ["cancellation", "alteration", "quick-buy", "buy"]

CUSTOMERS_TABLE = "mf_demo_customers"
TRANSACTIONS_TABLE = "mf_demo_transactions"
COUNTRIES_TABLE = "mf_demo_countries"

TRANSACTIONS_YAML_FILE = os.path.join(os.path.dirname(__file__), "sample_models/transactions.yaml")
CUSTOMERS_YAML_FILE = os.path.join(os.path.dirname(__file__), "sample_models/customers.yaml")
COUNTRIES_YAML_FILE = os.path.join(os.path.dirname(__file__), "sample_models/countries.yaml")


def build_dataframe(sql_client: SqlClient) -> Dict[str, pd.DataFrame]:
    """Builds random data with some logic.

    Args:
        sql_client: SqlClient used to format the dataframe

    Returns:
        A dict containing {table_name: dataframe of data}
    """
    transaction_data = [
        ("s59936437", "o1007", "c500001", 321.03, "quick-buy", "2022-03-22"),
        ("s59936438", "o1009", "c500003", 444.89, "cancellation", "2022-04-04"),
        ("s59936439", "o1002", "c500000", 129.68, "cancellation", "2022-03-15"),
        ("s59936440", "o1003", "c500000", 133.86, "buy", "2022-03-27"),
        ("s59936441", "o1003", "c500001", 296.73, "cancellation", "2022-03-16"),
        ("s59936442", "o1010", "c500000", 148.48, "cancellation", "2022-03-31"),
        ("s59936443", "o1003", "c500000", 183.12, "alteration", "2022-03-27"),
        ("s59936444", "o1008", "c500003", 353.81, "quick-buy", "2022-04-03"),
        ("s59936445", "o1005", "c500003", 116.76, "cancellation", "2022-03-31"),
        ("s59936446", "o1000", "c500000", 179.34, "cancellation", "2022-03-14"),
        ("s59936447", "o1004", "c500002", 273.16, "quick-buy", "2022-03-16"),
        ("s59936448", "o1004", "c500001", 499.59, "buy", "2022-03-15"),
        ("s59936449", "o1000", "c500002", 74.55, "buy", "2022-03-22"),
        ("s59936450", "o1006", "c500002", 460.83, "alteration", "2022-03-17"),
        ("s59936451", "o1001", "c500003", 147.92, "alteration", "2022-03-30"),
        ("s59936452", "o1008", "c500001", 259.58, "buy", "2022-03-25"),
        ("s59936453", "o1006", "c500003", 248.45, "alteration", "2022-04-02"),
        ("s59936454", "o1006", "c500002", 215.14, "cancellation", "2022-03-23"),
        ("s59936455", "o1004", "c500002", 353.66, "buy", "2022-03-08"),
        ("s59936456", "o1009", "c500002", 309.65, "alteration", "2022-03-31"),
        ("s59936457", "o1008", "c500002", 66.6, "quick-buy", "2022-03-25"),
        ("s59936458", "o1007", "c500000", 87.42, "buy", "2022-03-13"),
        ("s59936459", "o1003", "c500000", 100.4, "cancellation", "2022-04-01"),
        ("s59936460", "o1003", "c500003", 161.03, "alteration", "2022-03-30"),
        ("s59936461", "o1002", "c500002", 321.54, "quick-buy", "2022-04-02"),
        ("s59936462", "o1007", "c500000", 450.62, "buy", "2022-03-21"),
        ("s59936463", "o1009", "c500002", 266.9, "buy", "2022-03-28"),
        ("s59936464", "o1008", "c500000", 79.88, "quick-buy", "2022-03-10"),
        ("s59936465", "o1004", "c500001", 368.65, "buy", "2022-03-26"),
        ("s59936466", "o1002", "c500002", 7.66, "buy", "2022-03-09"),
        ("s59936467", "o1003", "c500000", 137.07, "quick-buy", "2022-03-15"),
        ("s59936468", "o1007", "c500000", 179.91, "buy", "2022-03-26"),
        ("s59936469", "o1002", "c500000", 208.37, "cancellation", "2022-03-11"),
        ("s59936470", "o1008", "c500003", 448.13, "cancellation", "2022-03-22"),
        ("s59936471", "o1010", "c500001", 426.25, "alteration", "2022-03-25"),
        ("s59936472", "o1003", "c500003", 223.34, "quick-buy", "2022-03-08"),
        ("s59936473", "o1001", "c500003", 225.86, "cancellation", "2022-03-18"),
        ("s59936474", "o1008", "c500001", 192.98, "buy", "2022-03-07"),
        ("s59936475", "o1008", "c500000", 130.81, "cancellation", "2022-03-12"),
        ("s59936476", "o1004", "c500002", 379.29, "alteration", "2022-03-31"),
        ("s59936477", "o1000", "c500001", 438.43, "quick-buy", "2022-03-29"),
        ("s59936478", "o1006", "c500002", 295.03, "quick-buy", "2022-03-07"),
        ("s59936479", "o1007", "c500003", 216.78, "cancellation", "2022-03-31"),
        ("s59936480", "o1005", "c500001", 40.82, "cancellation", "2022-03-23"),
        ("s59936481", "o1005", "c500002", 46.19, "buy", "2022-03-30"),
        ("s59936482", "o1009", "c500001", 347.62, "cancellation", "2022-03-29"),
        ("s59936483", "o1000", "c500002", 189.25, "cancellation", "2022-03-21"),
        ("s59936484", "o1001", "c500000", 179.11, "alteration", "2022-03-30"),
        ("s59936485", "o1010", "c500001", 258.72, "alteration", "2022-03-21"),
        ("s59936486", "o1001", "c500002", 91.55, "cancellation", "2022-03-10"),
    ]

    customer_data = [
        ("c500001", "FR", "2022-03-07"),
        ("c500003", "MX", "2022-03-08"),
        ("c500000", "US", "2022-03-10"),
        ("c500002", "CA", "2022-03-07"),
    ]
    return {
        CUSTOMERS_TABLE: make_df(
            sql_client=sql_client, columns=["id_customer", "country", "ds"], time_columns={"ds"}, data=customer_data
        ),
        TRANSACTIONS_TABLE: make_df(
            sql_client=sql_client,
            columns=[
                "id_transaction",
                "id_order",
                "id_customer",
                "transaction_amount_usd",
                "transaction_type_name",
                "ds",
            ],
            time_columns={"ds"},
            data=transaction_data,
        ),
        COUNTRIES_TABLE: make_df(sql_client=sql_client, columns=["country", "region"], data=COUNTRIES),
    }


def create_sample_data(sql_client: SqlClient, system_schema: str) -> bool:
    """Create tables with sample data into data warehouse."""

    if any(
        [
            sql_client.table_exists(SqlTable.from_string(f"{system_schema}.{CUSTOMERS_TABLE}")),
            sql_client.table_exists(SqlTable.from_string(f"{system_schema}.{TRANSACTIONS_TABLE}")),
            sql_client.table_exists(SqlTable.from_string(f"{system_schema}.{COUNTRIES_TABLE}")),
        ]
    ):
        # Do not create sample data if any of the table exists
        return False

    dummy_data = build_dataframe(sql_client)
    for table_name in dummy_data:
        sql_table = SqlTable.from_string(f"{system_schema}.{table_name}")
        sql_client.create_table_from_dataframe(sql_table=sql_table, df=dummy_data[table_name])
    return True


def remove_sample_tables(sql_client: SqlClient, system_schema: str) -> None:
    """Drop sample tables."""
    sql_client.drop_table(SqlTable.from_string(f"{system_schema}.{CUSTOMERS_TABLE}"))
    sql_client.drop_table(SqlTable.from_string(f"{system_schema}.{TRANSACTIONS_TABLE}"))
    sql_client.drop_table(SqlTable.from_string(f"{system_schema}.{COUNTRIES_TABLE}"))


def gen_sample_model_configs(dir_path: str, system_schema: str) -> None:
    """Generates the sample model configs to a specified directory."""

    with open(CUSTOMERS_YAML_FILE) as f:
        contents = Template(f.read()).substitute({"customers_table": f"{system_schema}.{CUSTOMERS_TABLE}"})
    with open(f"{dir_path}/{CUSTOMERS_TABLE}.yaml", "w") as file:
        file.write(contents)

    with open(TRANSACTIONS_YAML_FILE) as f:
        contents = Template(f.read()).substitute({"transactions_table": f"{system_schema}.{TRANSACTIONS_TABLE}"})
    with open(f"{dir_path}/{TRANSACTIONS_TABLE}.yaml", "w") as file:
        file.write(contents)

    with open(COUNTRIES_YAML_FILE) as f:
        contents = Template(f.read()).substitute({"countries_table": f"{system_schema}.{COUNTRIES_TABLE}"})
    with open(f"{dir_path}/{COUNTRIES_TABLE}.yaml", "w") as file:
        file.write(contents)
