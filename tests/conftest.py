import logging
import pytest

from symphonizer.composition import DAGNote

logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("nuropb").setLevel(logging.INFO)
logging.getLogger("urllib").setLevel(logging.WARNING)


@pytest.fixture(scope="function")
def sample_graph():
    dag_a = DAGNote("A")
    dag_b = DAGNote("B")
    dag_c = DAGNote("C")
    dag_d = DAGNote("D")
    return {
        dag_d: {dag_b, dag_c},
        dag_c: {dag_a},
        dag_b: {dag_a},
    }
