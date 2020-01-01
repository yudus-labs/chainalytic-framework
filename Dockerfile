FROM ubuntu:18.04

ENV PATH /opt/conda/bin:$PATH

RUN apt update --fix-missing && \
    apt install -y wget bzip2 ca-certificates && \
    apt install -y libsecp256k1-dev libleveldb-dev pkg-config build-essential && \
    apt clean

RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc

RUN mkdir chainalytic
WORKDIR /chainalytic
COPY src src
COPY launch.py launch.py
COPY MANIFEST.in MANIFEST.in
COPY README.adoc README.adoc
COPY setup.py setup.py

RUN python -m pip install -e .

ENTRYPOINT /opt/conda/bin/python launch.py --keep-running
