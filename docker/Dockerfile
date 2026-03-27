FROM python:3.11-slim

LABEL maintainer="your@email.com"
LABEL description="国内ETF智能分析系统"

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# 安装Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY . .

# 创建报告目录
RUN mkdir -p reports

# 暴露Web端口
EXPOSE 8080

# 默认启动Web服务
CMD ["python", "main.py", "server"]
