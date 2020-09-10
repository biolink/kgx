# Contributing Guidelines

Welcome contributors! KGX is a open-source project built for addressing the challenges of working knowledge graphs and 
its various serializations. We welcome your suggestions, discussions and contributions to this project. 


## Getting started

Before you get started please be sure to review the [Contributing Guidelines](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md).

### Developing

Take a look at the project's [README.md](README.md) for instructions on setting up a dev environment. We recommend using 
Python `venv` for setting up your virtual environment. 

In addition, you can find the technical documentation at [kgx.readthedocs.io](https://kgx.readthedocs.io/)

#### Coding Styles

We try to conform to [PEP-8](https://www.python.org/dev/peps/pep-0008/) style guidelines, but we do not enforce 
them yet. This might changes in the future.

But we do recommend [NumPy-style docstrings](https://python-sprints.github.io/pandas/guide/pandas_docstring.html)
for any method you write. This facilitates automated generation of documentation.

### Testing

All tests are located in [tests](tests/) folder.
For running tests, we use `pytest`.


## Discussion

If you run into any issues or find certain functionality not documented/explained properly then feel free to 
raise a ticket in the project's [issue tracker](https://github.com/NCATS-Tangerine/kgx/issues). 
There are issue templates to capture certain types of issues.

## First Time Contributors

We appreciate your enthusiasm and welcome contributions to this project.  

## How to Submit Changes

We recommend using GitHub Fork and Pull workflow which involves the following principles,

### Principle 1: Work from a personal fork

Prior to adopting the workflow, a developer will perform a one-time setup to create a personal Fork of the repository 
and will subsequently perform their development and testing on a task-specific branch within their forked repository. 
This forked repository will be associated with that developer's GitHub account, and is distinct from the main (upstream) 
repository.

### Principle 2: Commit to personal branches of that fork

Changes will never be committed directly to the master branch on the main repository. Rather, they will be composed 
as branches within the developer's forked repository, where the developer can iterate and refine their code prior to 
submitting it for review.

### Principle 3: Propose changes via pull request of personal branches

Each set of changes will be developed as a task-specific branch in the developer's forked repository, and then a pull 
request will be created to develop and propose changes to the shared repository. This mechanism provides a way for 
developers to discuss, revise and ultimately merge changes from the forked repository into the main repository.

### Principle 4: Delete or ignore stale branches, but don't recycle merged ones

Once a pull request has been merged, the task-specific branch is no longer needed and may be deleted or ignored. 
It is bad practice to reuse an existing branch once it has been merged. Instead, a subsequent branch and pull-request 
cycle should begin when a developer switches to a different coding task.

You may create a pull request in order to get feedback, but if you wish to continue working on the branch, 
so state with "DO NOT MERGE YET" in the PR title OR mark the pull request as a Draft OR label the pull request 
with a 'Do Not Merge' label. 


## How to Report a Bug

We recommend making a new ticket for each bug that you encounter while working with KGX. Please be sure to provide
sufficient context for a bug you are reporting. There are [Issue Templates](https://github.com/NCATS-Tangerine/kgx/issues/new/choose) 
that you can use as a starting point.

## How to Request an Enhancement

We welcome request for enhancements and you can make these requestes via the issue tracker. Please be sure to provide
sufficient context to what the enhancement is trying to address, its utility, and how it's likely to be useful to you
and the broader community.


## Core Developer Guidelines

Core developers should follow these rules when processing pull requests:
- All PRs should originate from a fork
- Always wait for tests to pass before merging PRs
- Use "Squash and merge" to merge PRs

