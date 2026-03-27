FROM python:3.9-slim

WORKDIR /app

# 设置时区为上海
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY . .

# 默认启动 Web 界面管理器
CMD ["uvicorn", "web_server:app", "--host", "0.0.0.0", "--port", "8080"]