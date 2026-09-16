# Conductor-to-Refiner contract validation

## Scope

This validation is deliberately limited to the isolated starter workspace
created for this Refiner job.  It does not modify an existing estate repository,
deployment, or runtime service.

## Observed contract evidence

- Conductor supplied an authoritative requirements document to Refiner.
- The delivered contract requires `validation_report.md` and
  `test_contract.py` to exist in this repository.
- The required native verification command is `python -m pytest -q`.
- The requested hand-off is artifact generation and verification only; no
  rollout target or production change is in scope.

## Verification

Refiner runs `python -m pytest -q` after creating these artifacts.  A passing
result is the acceptance signal for this isolated contract check.  The tests
also check that this report retains the observed evidence and limitation
statements needed by a Conductor consumer.

## Limitations

This check validates the request/response contract and the presence and
usability of the supplied artifacts.  It does not prove that an unrelated
estate repository can be safely changed, deployed, or promoted through later
delivery stages.

## Recovery

If verification fails, retain this workspace and inspect the Refiner job log
and test output.  Correct only the isolated artifacts, rerun the required
pytest command, and do not promote or roll out the result until it passes.
