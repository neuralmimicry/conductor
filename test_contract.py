from pathlib import Path


REPORT = Path("validation_report.md")
TEST = Path("test_contract.py")


def test_required_artifacts_exist():
    assert REPORT.is_file()
    assert TEST.is_file()


def test_report_contains_usable_contract_evidence():
    report = REPORT.read_text(encoding="utf-8")
    assert "Observed contract evidence" in report
    assert "python -m pytest -q" in report
    assert "isolated starter workspace" in report


def test_report_records_scope_and_limitations():
    report = REPORT.read_text(encoding="utf-8")
    assert "## Scope" in report
    assert "## Limitations" in report
    assert "does not modify an existing estate repository" in report
