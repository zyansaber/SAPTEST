from flask import Flask, render_template, request, redirect, url_for, session
from sqlalchemy import create_engine, text
import pandas as pd

app = Flask(__name__)
app.secret_key = "secure_session"

# 使用 pyodbc 连接 Azure SQL
conn_str = (
    "mssql+pyodbc://sqladmin:Planning456!@sap-sqlserver.database.windows.net:1433/sapdb"
    "?driver=ODBC+Driver+17+for+SQL+Server&encrypt=yes"
)
engine = create_engine(conn_str)

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        session["username"] = username
        return redirect(url_for("dashboard"))
    return render_template("login.html")

@app.route("/dashboard")
def dashboard():
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
    df = pd.read_sql(text(query), engine)
    tables = df['TABLE_NAME'].tolist()
    return render_template("dashboard.html", tables=tables, user=session.get("username", "Unknown"))

@app.route("/view_table/<table_name>")
def view_table(table_name):
    query = f"SELECT TOP 100 * FROM {table_name}"
    df = pd.read_sql(text(query), engine)
    return render_template("view_table.html", table_name=table_name, table=df.to_html(classes='table table-striped', index=False), user=session.get("username", "Unknown"))

if __name__ == "__main__":
    app.run(debug=True)