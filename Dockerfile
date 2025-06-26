FROM python:3.9-slim

# 安装必要工具和 ODBC 驱动
RUN apt-get update &&     apt-get install -y gnupg2 curl apt-transport-https &&     curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - &&     curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list &&     apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev gcc g++ &&     apt-get clean

# 安装 Python 依赖
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY . .

# 设置启动命令
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:10000"]