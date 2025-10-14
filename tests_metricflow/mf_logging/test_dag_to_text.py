from __future__ import annotations

import logging
import textwrap
import threading
import time
from typing import List

from metricflow_semantics.dag.dag_to_text import MetricFlowDagTextFormatter
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.sql.sql_exprs import (
    SqlStringExpression,
)
from metricflow_semantics.sql.sql_table import SqlTable
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.string_helpers import mf_indent

from metricflow.sql.sql_plan import (
    SqlPlan,
    SqlSelectColumn,
)
from metricflow.sql.sql_select_node import SqlSelectStatementNode
from metricflow.sql.sql_table_node import SqlTableNode

logger = logging.getLogger(__name__)


def test_multithread_dag_to_text() -> None:
    """Test that dag_to_text() works correctly in a multithreading context."""
    num_threads = 4
    thread_outputs: List[str] = []

    # Using a nested structure w/ small max_line_length to force recursion / cover recursive width tracking.
    dag_to_text_formatter = MetricFlowDagTextFormatter(max_width=1)
    dag = SqlPlan(
        plan_id=DagId("plan"),
        render_node=SqlSelectStatementNode.create(
            description="test",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlStringExpression.create("'foo'"),
                    column_alias="bar",
                ),
            ),
            from_source=SqlTableNode.create(sql_table=SqlTable(schema_name="schema", table_name="table")),
            from_source_alias="src",
        ),
    )

    def _run_mf_pformat() -> None:
        current_thread = threading.current_thread()
        logger.debug(LazyFormat(lambda: f"In {current_thread} - Starting .dag_to_text()"))
        # Sleep a little bit so that all threads are likely to be running simultaneously.
        time.sleep(0.5)
        try:
            output = dag_to_text_formatter.dag_to_text(dag)
            logger.debug(LazyFormat(lambda: f"in {current_thread} - Output is:\n{mf_indent(output)}"))
            thread_outputs.append(output)
            logger.debug(LazyFormat(lambda: f"In {current_thread} - Successfully finished .dag_to_text()"))
        except Exception:
            logger.exception(f"In {current_thread} - Exiting due to an exception")

    threads = tuple(threading.Thread(target=_run_mf_pformat) for _ in range(num_threads))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    expected_thread_output = textwrap.dedent(
        """\
        <SqlPlan>
            <SqlSelectStatementNode>
                <!-- description =  -->
                <!--   'test' -->
                <!-- node_id =          -->
                <!--   NodeId(          -->
                <!--     id_str='ss_0', -->
                <!--   )                -->
                <!-- col0 =                                                      -->
                <!--   SqlSelectColumn(                                          -->
                <!--     expr=SqlStringExpression(node_id=str_0 sql_expr='foo'), -->
                <!--     column_alias='bar',                                     -->
                <!--   )                                                         -->
                <!-- from_source =                 -->
                <!--   SqlTableNode(node_id=tfc_0) -->
                <!-- where =  -->
                <!--   None -->
                <!-- distinct =  -->
                <!--   False -->
                <SqlTableNode>
                    <!-- description =      -->
                    <!--   ('Read '         -->
                    <!--    'from '         -->
                    <!--    'schema.table') -->
                    <!-- node_id =           -->
                    <!--   NodeId(           -->
                    <!--     id_str='tfc_0', -->
                    <!--   )                 -->
                    <!-- table_id =       -->
                    <!--   'schema.table' -->
                </SqlTableNode>
            </SqlSelectStatementNode>
        </SqlPlan>
        """
    ).rstrip()
    assert thread_outputs == [expected_thread_output for _ in range(num_threads)]
