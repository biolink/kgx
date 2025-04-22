# Instructions for building KGX documentation

These instructions are for the core developers of KGX.

Documentation source:

* [docs/ folder](https://github.com/biolink/kgx/tree/master/docs)

We use the sphinx framework.

## Instructions

To build the docs locally, first make sure you have the development dependencies installed.  Run:

```bash
poetry install
```

Then use the make to build the documentation:

```bash
make docs
```

This will build docs in `_build/html/`. You can check these with your browser.

If you don't have make (on Windows) you can build the docs by:

```bash
cd docs
poetry run make html
```

New versions of the documentation are published to GitHub pages by a workflow job for every merge to main.

## IMPORTANT

**never** run `make html` directly

If you do this then docstrings from KGX will not be included.
Always check the generator docs to ensure command line options are present.
