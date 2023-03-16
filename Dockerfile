FROM python:3.9
MAINTAINER  Sierra Moxon "smoxon@lbl.gov"

# Clone repository
RUN git clone https://github.com/biolink/kgx

# Setup
RUN cd kgx && git checkout tags/2.0.0 && poetry install

# Make data directory
RUN mkdir data

WORKDIR /kgx

CMD ["/bin/bash"]
