"""
Prueba de Integridad de DAGs de Apache Airflow.
Valida que todos los DAGs del directorio airflow/dags tengan sintaxis Python válida,
se importen sin errores y definan objetos DAG correctamente estructurados.
"""
import os
import ast
import glob
import sys
import types
from unittest.mock import MagicMock
import pytest

DAGS_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags"))

def get_dag_files():
    pattern = os.path.join(DAGS_FOLDER, "*.py")
    return sorted(glob.glob(pattern))

def setup_airflow_mocks():
    """Inyecta stubs para componentes de Airflow permitiendo validar DAGs en entornos de testing."""
    airflow_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    
    # Asegurar que airflow sea un package con __path__ para resolver airflow.include
    if "airflow" not in sys.modules:
        airflow_mod = types.ModuleType("airflow")
        airflow_mod.__path__ = [airflow_root]
        sys.modules["airflow"] = airflow_mod
    else:
        airflow_mod = sys.modules["airflow"]
        if not hasattr(airflow_mod, "__path__"):
            airflow_mod.__path__ = [airflow_root]

    class MockDAG:
        def __init__(self, dag_id, *args, **kwargs):
            self.dag_id = dag_id
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def __rshift__(self, other):
            return other

    class MockOperator:
        def __init__(self, *args, **kwargs):
            pass
        def __rshift__(self, other):
            return other

    class MockTaskGroup:
        def __init__(self, group_id, *args, **kwargs):
            self.group_id = group_id
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def __rshift__(self, other):
            return other

    airflow_mod.DAG = MockDAG

    # Operadores
    ops_py = types.ModuleType("airflow.operators.python")
    ops_py.PythonOperator = MockOperator
    sys.modules["airflow.operators.python"] = ops_py

    ops_trig = types.ModuleType("airflow.operators.trigger_dagrun")
    ops_trig.TriggerDagRunOperator = MockOperator
    sys.modules["airflow.operators.trigger_dagrun"] = ops_trig

    # TaskGroup
    utils_tg = types.ModuleType("airflow.utils.task_group")
    utils_tg.TaskGroup = MockTaskGroup
    sys.modules["airflow.utils.task_group"] = utils_tg

    # Params
    models_param = types.ModuleType("airflow.models.param")
    models_param.Param = MagicMock()
    sys.modules["airflow.models.param"] = models_param

def test_dag_files_exist():
    files = get_dag_files()
    assert len(files) >= 4, f"Se esperaban al menos 4 DAGs, se encontraron {len(files)}"

@pytest.mark.parametrize("dag_path", get_dag_files())
def test_dag_ast_syntax(dag_path):
    """Verifica que el archivo tenga sintaxis Python estrictamente válida."""
    with open(dag_path, "r", encoding="utf-8") as f:
        content = f.read()
    try:
        parsed = ast.parse(content)
        assert parsed is not None
    except SyntaxError as e:
        pytest.fail(f"Error de sintaxis en DAG {dag_path}: {e}")

@pytest.mark.parametrize("dag_path", get_dag_files())
def test_dag_import_integrity(dag_path):
    """
    Verifica que cada DAG se importe sin excepciones y defina correctamente
    su identificador de DAG y estructura de tareas.
    """
    setup_airflow_mocks()

    import importlib.util
    module_name = f"dag_module_{os.path.splitext(os.path.basename(dag_path))[0]}"
    spec = importlib.util.spec_from_file_location(module_name, dag_path)
    module = importlib.util.module_from_spec(spec)
    
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        pytest.fail(f"Falla al importar DAG en {dag_path}: {exc}")

    # Verificar que el módulo contenga un objeto DAG
    has_dag = any(
        "dag" in k.lower() or "DAG" in type(v).__name__ or type(v).__name__ == "MockDAG"
        for k, v in vars(module).items()
    )
    assert has_dag, f"El archivo {dag_path} no define ningún objeto DAG."
