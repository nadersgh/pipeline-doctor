from pipeline_doctor.evals.cases import EvalCase, EvalSplit, InfraFailureKind
from pipeline_doctor.incidents.models import FixKind, RootCauseCategory

# Data, code, and git history stay green; only the run fails. Any code fix or confident culprit is a false positive.
_NO_FAULT_CASES: list[EvalCase] = [
    EvalCase(
        case_id="nofault_warehouse_lock",
        title="Another process holds the DuckDB write lock during the run",
        category=RootCauseCategory.UNKNOWN,
        culprit_relation="",
        accepted_fix_kinds={FixKind.OPERATIONAL},
        split=EvalSplit.HOLDOUT,
        infra_failure=InfraFailureKind.WAREHOUSE_LOCK,
    ),
    EvalCase(
        case_id="nofault_worker_killed",
        title="The Airflow worker is OOM-killed mid dbt build",
        category=RootCauseCategory.UNKNOWN,
        culprit_relation="",
        accepted_fix_kinds={FixKind.OPERATIONAL},
        split=EvalSplit.HOLDOUT,
        infra_failure=InfraFailureKind.WORKER_KILLED,
    ),
    EvalCase(
        case_id="nofault_network_timeout",
        title="The warehouse connection is reset mid dbt build and Airflow queues a retry",
        category=RootCauseCategory.UNKNOWN,
        culprit_relation="",
        accepted_fix_kinds={FixKind.OPERATIONAL},
        split=EvalSplit.HOLDOUT,
        infra_failure=InfraFailureKind.NETWORK_TIMEOUT,
    ),
    EvalCase(
        case_id="nofault_heartbeat_lost",
        title="The scheduler marks the running task a zombie after its heartbeat stops",
        category=RootCauseCategory.UNKNOWN,
        culprit_relation="",
        accepted_fix_kinds={FixKind.OPERATIONAL},
        split=EvalSplit.HOLDOUT,
        infra_failure=InfraFailureKind.HEARTBEAT_LOST,
    ),
]


def list_no_fault_cases() -> list[EvalCase]:
    return list(_NO_FAULT_CASES)
