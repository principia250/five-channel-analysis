# Airflow公式イメージをベースに、MeCabをインストールしたカスタムイメージを作成
FROM apache/airflow:3.1.5

# ルートユーザーでシステムパッケージをインストール
USER root

# MeCabとその依存関係、NEologdビルドに必要なツールをインストール
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        mecab \
        libmecab-dev \
        mecab-ipadic-utf8 \
        git \
        make \
        curl \
        xz-utils \
        file \
        sudo \
        patch && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# NEologdインストール用のディレクトリを作成
RUN mkdir -p /opt/neologd && \
    mkdir -p /opt/mecab-ipadic-neologd && \
    chown -R airflow:root /opt/neologd && \
    chown -R airflow:root /opt/mecab-ipadic-neologd

# Airflowユーザーに戻す
USER airflow

# Pythonパッケージをインストール
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt
