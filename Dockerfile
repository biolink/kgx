FROM python:3.7
MAINTAINER  Deepak Unni "deepak.unni3@gmail.com"

# Clone repository
RUN git clone https://github.com/NCATS-Tangerine/kgx

# Setup
RUN cd kgx && python setup.py install

# Make data directory
RUN mkdir data

WORKDIR /kgx

CMD ["/bin/bash"]
