"""Static validation tests for Airflow DAG files.

These tests parse the DAG files as Python AST without importing Airflow,
so they run in any environment that has Python — no Airflow install needed.
They catch the most common DAG bugs: syntax errors, missing @dag decorators,
missing asset declarations, and missing pipeline instantiation.

For full DagBag-based validation (which requires Airflow installed),
see tests/integration/ when that suite is added.
"""

import ast
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"

EXPECTED_DAGS = {
    "staging_brasileirao_ingestion.py": {
        "dag_id": "staging_brasileirao_ingestion",
        "pipeline_func": "staging_brasileirao_pipeline",
        "asset_uri": "s3://staging/brasileirao",
    },
    "bronze_silver_brasileirao_processing.py": {
        "dag_id": "bronze_silver_brasileirao_processing",
        "pipeline_func": "bronze_silver_brasileirao_pipeline",
        "asset_uri": "iceberg://nessie/silver/brasileirao",
    },
    "gold_dbt_brasileirao_processing.py": {
        "dag_id": "gold_dbt_brasileirao_processing",
        "pipeline_func": "gold_dbt_brasileirao_pipeline",
        "asset_uri": "iceberg://nessie/gold/brasileirao",
    },
    "iceberg_maintenance.py": {
        "dag_id": "iceberg_maintenance",
        "pipeline_func": "iceberg_maintenance_pipeline",
        # Non-asset DAG: maintenance runs on a @weekly cron, does not consume
        # or emit data assets. asset_uri is intentionally None so the parametrized
        # asset test below skips it.
        "asset_uri": None,
    },
}


def _parse(filename: str) -> ast.Module:
    """Parse a DAG file and return its AST module."""
    path = DAGS_DIR / filename
    return ast.parse(path.read_text(encoding="utf-8"))


def _collect_decorator_names(tree: ast.Module) -> list[str]:
    """Return the names of all decorators applied to top-level functions."""
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Name):
                    names.append(dec.func.id)
                elif isinstance(dec, ast.Name):
                    names.append(dec.id)
    return names


def _collect_string_constants(tree: ast.Module) -> list[str]:
    """Return every string literal in the module."""
    return [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def _collect_top_level_calls(tree: ast.Module) -> list[str]:
    """Return names of functions called at module top level (not inside other defs)."""
    calls = []
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Name):
                calls.append(call.func.id)
    return calls


class TestDagFilesExist:
    """All expected DAG files must be present on disk."""

    @pytest.mark.parametrize("filename", list(EXPECTED_DAGS.keys()))
    def test_dag_file_exists(self, filename):
        assert (DAGS_DIR / filename).is_file(), f"missing DAG file: {filename}"


class TestDagSyntax:
    """All DAG files must be valid Python."""

    @pytest.mark.parametrize("filename", list(EXPECTED_DAGS.keys()))
    def test_dag_parses_as_python(self, filename):
        _parse(filename)


class TestDagStructure:
    """Each DAG file must declare its @dag decorator, asset, and instantiate the pipeline."""

    @pytest.mark.parametrize(
        "filename,spec",
        [(f, s) for f, s in EXPECTED_DAGS.items()],
    )
    def test_has_dag_decorator(self, filename, spec):
        tree = _parse(filename)
        decorators = _collect_decorator_names(tree)
        assert "dag" in decorators, f"{filename} is missing the @dag decorator"

    @pytest.mark.parametrize(
        "filename,spec",
        [(f, s) for f, s in EXPECTED_DAGS.items()],
    )
    def test_asset_uri_declared(self, filename, spec):
        if spec["asset_uri"] is None:
            pytest.skip(f"{filename} is a non-asset DAG (cron-scheduled)")
        tree = _parse(filename)
        strings = _collect_string_constants(tree)
        assert spec["asset_uri"] in strings, f"{filename} does not reference its expected asset URI {spec['asset_uri']}"

    @pytest.mark.parametrize(
        "filename,spec",
        [(f, s) for f, s in EXPECTED_DAGS.items()],
    )
    def test_dag_id_declared(self, filename, spec):
        tree = _parse(filename)
        strings = _collect_string_constants(tree)
        assert spec["dag_id"] in strings, f"{filename} does not declare dag_id={spec['dag_id']}"

    @pytest.mark.parametrize(
        "filename,spec",
        [(f, s) for f, s in EXPECTED_DAGS.items()],
    )
    def test_pipeline_is_instantiated(self, filename, spec):
        """The DAG function must be called at module top-level to register the DAG."""
        tree = _parse(filename)
        top_calls = _collect_top_level_calls(tree)
        assert spec["pipeline_func"] in top_calls, (
            f"{filename} defines {spec['pipeline_func']} but never calls it at module level "
            "— the DAG will not be registered with Airflow"
        )


class TestFailureCallbacks:
    """Every DAG should wire up an on_failure_callback for observability."""

    @pytest.mark.parametrize("filename", list(EXPECTED_DAGS.keys()))
    def test_wires_failure_callback(self, filename):
        """Each DAG must bind ``on_failure_callback`` — either as a local function
        or as an assignment (e.g. ``on_failure_callback = build_failure_callback(...)``).
        """
        tree = _parse(filename)
        func_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assigned_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        assigned_names.add(target.id)
        assert "on_failure_callback" in (func_names | assigned_names), f"{filename} does not bind on_failure_callback"


class TestSparkWorkerPool:
    """Every Spark task across all DAGs must run in the shared single-slot
    ``spark_worker`` pool, so only one Spark job hits the 2g worker at a time
    (B-02 — real isolation, not just maintenance serialized against itself)."""

    # Every DAG that submits Spark jobs to the shared worker.
    _SPARK_DAGS = (
        "iceberg_maintenance.py",
        "bronze_silver_brasileirao_processing.py",
    )

    def test_pool_name_literal_present(self):
        """Each Spark DAG must reference the literal 'spark_worker'.

        Frozen as a contract: the docker-compose scheduler bootstraps a pool
        with this exact name, so renaming the Python constant without updating
        the compose command would silently leave the tasks queued forever.
        """
        for filename in self._SPARK_DAGS:
            strings = _collect_string_constants(_parse(filename))
            assert "spark_worker" in strings, (
                f"{filename} must reference the literal 'spark_worker' — "
                "the docker-compose scheduler creates a pool with this exact name."
            )

    def test_every_spark_submit_sets_pool(self):
        """Every SparkSubmitOperator must set ``pool`` to the shared pool.

        Without it the operator falls back to default_pool and competes for the
        single 2g worker, which is exactly the concurrent-OOM B-02 closes.
        """
        for filename in self._SPARK_DAGS:
            submits = [
                node
                for node in ast.walk(_parse(filename))
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "SparkSubmitOperator"
            ]
            assert submits, f"{filename} has no SparkSubmitOperator (test assumption broke)"
            for node in submits:
                kwargs = {kw.arg: kw.value for kw in node.keywords}
                assert "pool" in kwargs, f"a SparkSubmitOperator in {filename} must set pool=..."
                pool_arg = kwargs["pool"]
                if isinstance(pool_arg, ast.Constant):
                    assert pool_arg.value == "spark_worker"
                elif isinstance(pool_arg, ast.Name):
                    assert pool_arg.id == "SPARK_WORKER_POOL"
                else:
                    pytest.fail(f"unexpected AST node for pool kwarg in {filename}: {ast.dump(pool_arg)}")


class TestNessieBranchPropagation:
    """A1 regression: the Bronze/Silver DAG must propagate the Nessie branch +
    execution date from ``create_branch``'s XCom — never recompute them per task.

    A per-task recompute (the old ``NESSIE_BRANCH_TEMPLATE`` from
    ``dag_run.logical_date`` with a UTC ``now()`` fallback) could derive a
    different date/branch than ``create_branch`` (which uses America/Sao_Paulo):
    across the midnight boundary, or any evening BRT run. The Spark jobs would
    then write to a branch ``create_branch`` never made, and merge/cleanup would
    act on an orphan. These tests freeze the single-source contract.
    """

    DAG_FILE = "bronze_silver_brasileirao_processing.py"

    def _spark_submit_arg_lists(self) -> list[ast.AST]:
        tree = _parse(self.DAG_FILE)
        arg_lists = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "SparkSubmitOperator":
                kwargs = {kw.arg: kw.value for kw in node.keywords}
                if "application_args" in kwargs:
                    arg_lists.append(kwargs["application_args"])
        return arg_lists

    def test_spark_jobs_reference_xcom_constants(self):
        """Every Spark job passes the XCom-backed constants as its args."""
        arg_lists = self._spark_submit_arg_lists()
        assert arg_lists, "expected SparkSubmitOperator(s) with application_args"
        for args in arg_lists:
            names = {n.id for n in ast.walk(args) if isinstance(n, ast.Name)}
            assert "XCOM_NESSIE_BRANCH" in names, "Spark --nessie-ref must use the XCom branch constant"
            assert "XCOM_EXECUTION_DATE" in names, "Spark execution_date must use the XCom date constant"

    def test_xcom_constants_pull_from_create_branch(self):
        """Those constants must be templates pulling create_branch's XCom dict."""
        tree = _parse(self.DAG_FILE)
        consts: dict[str, str] = {}
        for node in tree.body:
            if (
                isinstance(node, ast.Assign)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
            ):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        consts[target.id] = node.value.value
        assert "xcom_pull(task_ids='create_branch')['branch']" in consts.get("XCOM_NESSIE_BRANCH", "")
        assert "xcom_pull(task_ids='create_branch')['execution_date']" in consts.get("XCOM_EXECUTION_DATE", "")

    def test_no_per_task_date_recompute(self):
        """The recompute anti-pattern must not come back in any DAG template.

        The only date derivation lives in ``create_branch`` (which uses
        ``pendulum.now`` as the single documented fallback for a missing ``ds``).
        We scan string *literals* (the Jinja templates) rather than the raw
        source so explanatory comments may still mention the anti-pattern they
        replaced.
        """
        blob = " ".join(_collect_string_constants(_parse(self.DAG_FILE)))
        assert "dag_run.logical_date" not in blob, "no per-task recompute from logical_date in any template"
        assert "macros.datetime.now" not in blob, "no per-task recompute via macros.datetime.now"
