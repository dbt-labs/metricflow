from __future__ import annotations

import logging
import textwrap
import threading
import time
from typing import List

from metricflow.dag.dag_to_text import MetricFlowDagTextFormatter
from metricflow.dag.mf_dag import DagId
from metricflow.dataflow.sql_table import SqlTable
from metricflow.mf_logging.formatting import indent
from metricflow.sql.sql_exprs import (
    SqlStringExpression,
)
from metricflow.sql.sql_plan import SqlQueryPlan, SqlSelectColumn, SqlSelectStatementNode, SqlTableFromClauseNode

logger = logging.getLogger(__name__)


def test_multithread_dag_to_text() -> None:
    """Test that dag_to_text() works correctly in a multithreading context."""
    num_threads = 4
    thread_outputs: List[str] = []

    # Using a nested structure w/ small max_line_length to force recursion / cover recursive width tracking.
    dag_to_text_formatter = MetricFlowDagTextFormatter(max_width=1)
    dag = SqlQueryPlan(
        plan_id=DagId("plan"),
        render_node=SqlSelectStatementNode(
            description="test",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlStringExpression("'foo'"),
                    column_alias="bar",
                ),
            ),
            from_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="schema", table_name="table")),
            from_source_alias="src",
            joins_descs=(),
            group_bys=(),
            order_bys=(),
        ),
    )

    def _run_mf_pformat() -> None:  # noqa: D
        current_thread = threading.current_thread()
        logger.debug(f"In {current_thread} - Starting .dag_to_text()")
        # Sleep a little bit so that all threads are likely to be running simultaneously.
        time.sleep(0.5)
        try:
            output = dag_to_text_formatter.dag_to_text(dag)
            logger.debug(f"in {current_thread} - Output is:\n{indent(output)}")
            thread_outputs.append(output)
            logger.debug(f"In {current_thread} - Successfully finished .dag_to_text()")
        except Exception:
            logger.exception(f"In {current_thread} - Exiting due to an exception")

    threads = tuple(threading.Thread(target=_run_mf_pformat) for _ in range(num_threads))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    expected_thread_output = textwrap.dedent(
        """\
        <SqlQueryPlan>
            <SqlSelectStatementNode>
                <!-- description =  -->
                <!--   test -->
                <!-- node_id =  -->
                <!--   ss_0 -->
                <!-- col0 =                                                                                        -->
                <!--   SqlSelectColumn(expr=SqlStringExpression(node_id=str_0 sql_expr='foo'), column_alias='bar') -->
                <!-- from_source =                           -->
                <!--   SqlTableFromClauseNode(node_id=tfc_0) -->
                <!-- where =  -->
                <!--   None -->
                <!-- distinct =  -->
                <!--   False -->
                <SqlTableFromClauseNode>
                    <!-- description =            -->
                    <!--   Read from schema.table -->
                    <!-- node_id =  -->
                    <!--   tfc_0 -->
                    <!-- table_id =     -->
                    <!--   schema.table -->
                </SqlTableFromClauseNode>
            </SqlSelectStatementNode>
        </SqlQueryPlan>
        """
    ).rstrip()
    assert thread_outputs == [expected_thread_output for _ in range(num_threads)]
