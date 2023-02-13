## Release instructions

This section is only relevant for core developers.

### Update Dockerfile

Update Dockerfile to use the new tag.

### Update Documentation

Update KGX documentation to be consistent with any changes or new additions to the codebase.

### Make a new release tag

Commiting changes and making a new release tag.

```sh
TAG=`python setup.py --version`
git add kgx/__init__.py
git add setup.py
git add CHANGELOG.md
git add Dockerfile
git add docs/conf.py
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
docker build --no-cache -f Dockerfile --tag biolink/kgx:latest .
docker build --no-cache -f Dockerfile --tag biolink/kgx:$TAG .
```

Once the image is built, be sure to push to Dockerhub:

```sh
docker push biolink/kgx:latest
docker push biolink/kgx:$TAG
```

**Note:** It is important to have a fresh clone of the repository to avoid unnecessary files being included in the new Docker image.
