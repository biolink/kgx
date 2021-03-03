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


### Update Documentation

Update KGX documentation to be consistent with any changes or new additions to the codebase.

Also be sure to update the version in [docs/conf.py]() to ensure that the right version is being displayed when the documentation is rendered.

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

Also, be sure to install [twine](https://pypi.org/project/twine/) and [wheel](https://pypi.org/project/wheel/).

Now, run the following commands:

```sh
rm -rf dist/
python setup.py sdist bdist_wheel bdist_egg
twine upload --repository-url https://upload.pypi.org/legacy/ --username PYPI_USERNAME dist/*
```

### Release on GitHub

Go to [https://github.com/biolink/kgx/releases/](), edit the latest release and add the contents from [CHANGELOG.md]() corresponding to that release.


### Build Docker container

Build and push the Docker image for the new version of KGX.

First have a fresh clone of the KGX GitHub repository, and then build the Docker image:
```sh
git clone https://github.com/biolink/kgx
cd kgx
docker docker build --no-cache -f Dockerfile --tag biolink/kgx:latest .
docker docker build --no-cache -f Dockerfile --tag biolink/kgx:$TAG .
```

Once the image is built, be sure to push to Dockerhub:

```sh
docker push biolink/kgx:latest
docker push biolink/kgx:$TAG
```

**Note:** It is important to have a fresh clone of the repository to avoid unnecessary files being included in the new Docker image.
