FROM node:10-stretch

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y \
  yasm \
  python \
  python3 \
  python3-pip \
  gcc \
  g++ \
  cmake \
  make \
  curl \
  wget \
  apt-transport-https \
  m4 \
  zip \
  unzip \
  vim \
  build-essential

RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list
RUN apt-get update && apt-get install yarn -y

RUN git clone https://github.com/Athena-MHMD/SCALE-MAMBA.git

WORKDIR /SCALE-MAMBA
RUN ./install_dependencies.sh /local

ENV PATH="/local/openssl/bin/:${PATH}"
ENV C_INCLUDE_PATH="/local/openssl/include/:${C_INCLUDE_PATH}"
ENV CPLUS_INCLUDE_PATH="/local/openssl/include/:${CPLUS_INCLUDE_PATH}"
ENV LIBRARY_PATH="/local/openssl/lib/:${LIBRARY_PATH}"
ENV LD_LIBRARY_PATH="/local/openssl/lib/:${LD_LIBRARY_PATH}"
ENV C_INCLUDE_PATH="/local/mpir/include/:${C_INCLUDE_PATH}"
ENV CPLUS_INCLUDE_PATH="/local/mpir/include/:${CPLUS_INCLUDE_PATH}"
ENV LIBRARY_PATH="/local/mpir/lib/:${LIBRARY_PATH}"
ENV LD_LIBRARY_PATH="/local/mpir/lib/:${LD_LIBRARY_PATH}"
ENV CPLUS_INCLUDE_PATH="/local/cryptopp/include/:${CPLUS_INCLUDE_PATH}"
ENV LIBRARY_PATH="/local/cryptopp/lib/:${LIBRARY_PATH}"
ENV LD_LIBRARY_PATH="/local/cryptopp/lib/:${LD_LIBRARY_PATH}"

RUN cp CONFIG CONFIG.mine

RUN echo 'ROOT = /SCALE-MAMBA' >> CONFIG.mine
RUN echo 'OSSL = /local/openssl' >> CONFIG.mine

WORKDIR /SCALE-MAMBA/src
RUN make

COPY . /smpc-local-drive

WORKDIR /smpc-local-drive
RUN ./install.sh

RUN mkdir -p certs

WORKDIR /smpc-local-drive/scripts
RUN pip3 install -r requirements.txt

WORKDIR /smpc-local-drive

CMD ["node", "client.js"]
