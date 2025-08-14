import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, BigInteger, DECIMAL, Boolean, DateTime, MetaData, Text
from datetime import datetime


# SQLAlchemy Connection

engine = create_engine("mysql+pymysql://root:password@localhost:3306/article_db")


# Create Table if Not Exists

metadata = MetaData()

dq_monitoring_log = Table(
    'dq_monitoring_log', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('run_date', DateTime, nullable=False),
    Column('table_name', String(255)),
    Column('column_name', String(255)),
    Column('total_rows', BigInteger),
    Column('null_count', BigInteger),
    Column('completeness_pct', DECIMAL(5, 2)),
    Column('duplicate_count', BigInteger),
    Column('is_anomaly', Boolean),
    Column('anomaly_details', Text)
)

metadata.create_all(engine)  # Creates table if not exists


# 3. Load Data to Profile

df = pd.read_sql("SELECT * FROM news_article_table", engine)


# 4. Run DQ Checks & Insert

run_date = datetime.now()
results = []

for col in df.columns:
    total_rows = len(df)
    null_mask = ( df[col].isna() | (df[col] == "None") |  (df[col] == 0) | (df[col] == "0"))
    null_count = null_mask.sum() or 0
    completeness_pct = round(1 - (null_count / total_rows), 2)
    duplicate_count = df.duplicated(subset=[col]).sum()
    is_anomaly = completeness_pct < 0.90
    anomaly_details = f"Low completeness in {col}" if is_anomaly else None

    results.append({
        'run_date': run_date,
        'table_name': "my_data",
        'column_name': col,
        'total_rows': total_rows,
        'null_count': null_count,
        'completeness_pct': completeness_pct,
        'duplicate_count': duplicate_count,
        'is_anomaly': is_anomaly,
        'anomaly_details': anomaly_details
    })

# Convert to DataFrame and append to SQL
dq_df = pd.DataFrame(results)
dq_df.to_sql('dq_monitoring_log', engine, if_exists='append', index=False)

print(f"Inserted {len(dq_df)} monitoring records successfully.")
