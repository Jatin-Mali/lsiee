# LSIEE Security Policy

## Installation Security

- Install in a virtual environment.
- Do not install or run LSIEE as `root`.
- Verify the package name carefully.
- Run `venv/bin/python scripts/verify_installation.py` after installation.
- Run `venv/bin/python -m lsiee verify` after indexing or schema changes.
- Run `pip-audit` before release builds when available.

Recommended flow:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
venv/bin/python scripts/verify_installation.py
```

## Update Security

- Use normal `pip install --upgrade ...` workflows instead of auto-update logic.
- Review changelogs before upgrading.
- Back up LSIEE data before major upgrades.
- Re-run the full validation suite after dependency or schema changes.

## Supported Reporting

Security issues should include:

- affected version or commit
- operating system and Python version
- reproduction steps
- impact summary
- whether local data corruption or disclosure is involved

## Incident Response

Severity targets:

- `P0`: within 1 hour for data-loss, corruption, or code-execution paths
- `P1`: within 4 hours for sensitive-data exposure or severe availability loss
- `P2`: within 1 week for medium-severity integrity or privacy issues

Response checklist:

1. Confirm impact and affected versions.
2. Contain the issue or disable the vulnerable path.
3. Prepare and validate a patch.
4. Publish remediation guidance.
5. Add regression coverage and update the checklist below.

## Release Checklist

- `venv/bin/python scripts/verify_installation.py`
- `venv/bin/python -m lsiee verify`
- `venv/bin/pytest -q`
- `venv/bin/python -m black --check lsiee tests scripts`
- `venv/bin/python -m isort --check-only lsiee tests scripts`
- `venv/bin/python -m flake8 lsiee tests scripts`
- review pinned dependencies in `requirements.txt`
- verify `export`, `cleanup`, and `delete-all-data` against temp-local paths
- confirm logs redact secrets and terminal output strips control sequences

## Future Threat Areas

- Execution-layer features must keep `shell=False` and strict command allowlists.
- Any LLM-assisted feature must treat prompts, retrieved content, and model output as untrusted input.
- Releases should continue to pin dependencies and use supply-chain review tooling.
