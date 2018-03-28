FROM nvidia/cuda:9.0-cudnn7-runtime-ubuntu16.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-setuptools libgomp1 ocl-icd-opencl-dev libcurl3 && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install pip --upgrade && \
    pip3 install setuptools wheel --upgrade && \
    pip3 install tensorflow-gpu==1.6.0 && \
    echo "\n/usr/local/cuda-9.0/lib64" >> /etc/ld.so.conf && \
    ldconfig

ADD mining /home/ichaelm/mining