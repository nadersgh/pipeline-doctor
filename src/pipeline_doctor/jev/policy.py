from pipeline_doctor.incidents.models import FixKind, RootCauseCategory

_CODE = {FixKind.MODEL_CHANGE, FixKind.SCHEMA_PATCH}

# Which remedies are acceptable for each root cause. Data the source owner must repair is operational;
# code regressions are fixed in the model.
REMEDY_POLICY: dict[RootCauseCategory, set[FixKind]] = {
    RootCauseCategory.SOURCE_COLUMN_RENAMED: _CODE,
    RootCauseCategory.SOURCE_TYPE_CHANGED: _CODE,
    RootCauseCategory.SOURCE_NULL_SPIKE: {FixKind.OPERATIONAL, *_CODE},
    RootCauseCategory.SOURCE_DUPLICATE_ROWS: {FixKind.OPERATIONAL, *_CODE},
    RootCauseCategory.SOURCE_STALE: {FixKind.OPERATIONAL},
    RootCauseCategory.SOURCE_NEW_CATEGORY: {FixKind.OPERATIONAL, FixKind.TEST_CHANGE, FixKind.MODEL_CHANGE},
    RootCauseCategory.SOURCE_MISSING_ROWS: {FixKind.OPERATIONAL},
    RootCauseCategory.CODE_SQL_ERROR: {FixKind.MODEL_CHANGE},
    RootCauseCategory.CODE_JOIN_FANOUT: {FixKind.MODEL_CHANGE},
    RootCauseCategory.CODE_LOGIC_REGRESSION: {FixKind.MODEL_CHANGE},
    RootCauseCategory.UNKNOWN: {FixKind.OPERATIONAL},
}


def is_source_category(category: RootCauseCategory) -> bool:
    return category.value.startswith("source_")


def is_code_category(category: RootCauseCategory) -> bool:
    return category.value.startswith("code_")
