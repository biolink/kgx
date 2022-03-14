FROM python:3.7
MAINTAINER  Sierra Moxon "smoxon@lbl.gov"

# Clone repository
RUN git clone https://github.com/NCATS-Tangerine/kgx

# Setup
RUN cd kgx && git checkout tags/1.5.6 && pip install -r requirements.txt && python setup.py install

# Make data directory
RUN mkdir data

WORKDIR /kgx

CMD ["/bin/bash"]
