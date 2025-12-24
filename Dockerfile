# Airflow公式イメージをベースに、MeCabをインストールしたカスタムイメージを作成
FROM apache/airflow:3.1.5

# ルートユーザーでシステムパッケージをインストール
USER root

# MeCabとその依存関係をインストール
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        mecab \
        libmecab-dev \
        mecab-ipadic-utf8 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Airflowユーザーに戻す
USER airflow

# Pythonパッケージをインストール
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

