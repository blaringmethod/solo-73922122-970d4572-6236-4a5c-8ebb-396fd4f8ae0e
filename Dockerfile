FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY demo_feed ./demo_feed

RUN mkdir -p /data

EXPOSE 8000 8010

# 默认启动采集端；演示源服务在 compose 中用同一镜像以不同命令启动
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
