import pandas as pd
from sqlalchemy import create_engine

# 1. CSV 파일 불러오기
csv_path = "/Users/jenna/pobiga/B2_반도체/df_merged_0721.csv"
df = pd.read_csv(csv_path)

# 2. MariaDB 연결 정보 설정
user = 'root'
password = '000413'   # 너가 docker run 할 때 설정한 비번
host = 'localhost'
port = 3306
database = 'testdb'  # DB 없으면 아래에서 만들어줄 수도 있음

# 3. SQLAlchemy 엔진 생성
engine = create_engine(f"mysql+pymysql://jenna:000413@localhost:3306/testdb")

# 4. 업로드 (테이블 이름은 'df_merged')
df.to_sql(name='df_merged', con=engine, if_exists='replace', index=False)

print("CSV 데이터가 MariaDB에 성공적으로 업로드되었습니다!")