## Release instructions

This section is only relevant for core developers.

### Update version

1. First check whether the `__version__` in [`kgx/__init__.py`](kgx/__init__.py) matches with the latest tag or PyPI release. 
If the version is the same then you need to bump the version to make a new release. 
Follow [Semantic Versioning guidelines](https://semver.org/) to decide whether the bump in version is major or minor.

2. Now do the same for `VERSION` in [`setup.py`](setup.py), such that the version is in-sync with [`kgx/__init__.py`](kgx/__init__.py).


### Update Changelog.md

Now update Changelog.md and add the changes that this new release has.


### Update Dockerfile

Now update Dockerfile to use the new tag.


### Make a new release tag

Commiting changes and making a new release tag.

```sh
TAG=`python setup.py --version`
git add kgx/__init__.py
git add setup.py
git add CHANGELOG.md
git add Dockerfile
git commit --message="Bump version to $TAG in preparation of a release"
git push
git tag --annotate $TAG --message="Release $TAG"
git push --tags
  ```

### Release on PyPI

To ensure this is successful, make sure you have relevant permissions to KGX package on [PyPI](https://pypi.org/project/kgx/).

Also, be sure to install [twine](https://pypi.org/project/twine/) and [wheel](https://pypi.org/project/wheel/)

Now, run the following commands:

```sh
make cleandist
python setup.py sdist bdist_wheel bdist_egg
twine upload --repository-url https://upload.pypi.org/legacy/ --username PYPI_USERNAME dist/*
```

