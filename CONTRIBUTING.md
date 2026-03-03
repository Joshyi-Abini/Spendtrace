# Contributing

## Setup

1. Create a virtual environment.
2. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

## Development workflow

1. Make focused changes.
2. Run checks:

```bash
python -m pytest -q
python complete_example.py
```

3. Update docs for behavior changes.

## Pull request checklist

- [ ] Tests updated or added
- [ ] README updated if API/behavior changed
- [ ] Example scripts still run
- [ ] No sensitive data in code or logs
