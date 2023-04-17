
### Select a new semantic version for the release

### Make a github release using the web interface

https://github.com/biolink/kgx/releases/new

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

**Note:** It is important to have a fresh clone of the repository to avoid unnecessary files 
being included in the new Docker image.

### Release on PyPI

Will release based on the GitHub action in `.github/workflows/pypi-release.yml`
