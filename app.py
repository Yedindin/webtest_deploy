from flask import Flask, render_template, request
import pandas as pd
from sqlalchemy import create_engine
import itertools
import numpy as np

app = Flask(__name__)

# MariaDB 연결 정보 설정
db_url = "mysql+pymysql://root:000413@localhost:3306/testdb"
engine = create_engine(db_url)

def loadData():
    # 데이터 불러오기
    df = pd.read_sql("SELECT * FROM df_merged", engine)
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    today = df['Datetime'].max().date()
    month = today.month
    year = today.year

    return df, df['Datetime'], today, month, year

@app.route('/')
def dashboard():
    # 데이터 불러오기
    df, df['Datetime'], today, month, year = loadData()

    # 카드용 지표 계산
    today_rows = df[df['Datetime'].dt.date == today]
    cards = {
        '금일 생산량[단위:웨이퍼]': f"{len(today_rows)} / 108 개",
        '금일 불량 발생수[단위:칩]': f"{today_rows['Target'].sum()} / {len(today_rows)*520} 개",
        '당월 목표 생산량[단위:웨이퍼]': f"{len(df[(df['Datetime'].dt.month == month) & (df['Datetime'].dt.year == year)])} / {108 * 30} 개",
        '분기 목표 생산량[단위:웨이퍼]': f"{len(df[(df['Datetime'].dt.quarter == pd.Timestamp(today).quarter) & (df['Datetime'].dt.year == year)])} / {108 * 30 * 3} 개"
    }

    # 7일간 불량률 추이
    recent_week = df[df['Datetime'] >= pd.Timestamp(today) - pd.Timedelta(days=6)]
    defectRateData = (
        recent_week.groupby(recent_week['Datetime'].dt.strftime('%Y-%m-%d'))
        .agg(produced=('Target', 'count'), defective=('Target', 'sum'))
        .reset_index()
    )
    defectRateData['defect_rate'] = round(defectRateData['defective'] / (defectRateData['produced'] * 520) * 100, 2)

    # 월별 부하 (가장 최근 월 기준)
    recent_month = df[(df['Datetime'].dt.month == month) & (df['Datetime'].dt.year == year)]
    loadByRouteData = (
        recent_month.groupby('Route').size().reset_index(name='count').sort_values('count', ascending=False)
    )

    # 월별 생산량 집계
    df['Month'] = df['Datetime'].dt.to_period('M').astype(str)
    monthly_counts = df.groupby('Month').size().reset_index(name='Count')

    # Chart.js용 데이터 구성
    chart_data = {
        'labels': monthly_counts['Month'].tolist(),
        'counts': monthly_counts['Count'].tolist()
    }

    return render_template(
        'dashboard.html',
        cards=cards,
        defectRateData=defectRateData,
        loadByRouteData=loadByRouteData,
        monthly_chart=chart_data
    )

    # 관리 한계 조건
def check_limit(row):
    limits = {
        'Temp_OXid_oxi': (1275, 1348),
        'ppm_oxi': [(20.75, 28.17), (46.07, 50.06)],
        'Thin F4_etch': [(13, 151), (680, 687)],
        'Etching rate_etch': [(5012, 5028), (5563, 5694)],
        'Flux160s_ion': (1.2e18, 1.4e18),
        'input_Energy_ion': [(32773, 33675), (29604, 30152)],
        'temp_softbake_sb': [(86.23, 88.36), (95.48, 96.77)],
        'temp_HMDS_bake_sb': [(68.75, 190.93), (72.97, 73.34)],
        'spin1_sb': [(5012, 5028), (5563, 5694)],
        'spin3_sb': [(12.41, 16.5), (47.45, 61.88)],
        'Energy_Exposure_litho': (111.702, 112.223)
    }
    highlight = {}
    for key in limits:
        val = row.get(key, None)
        if val is None: continue
        if isinstance(limits[key], tuple):
            low, high = limits[key]
            highlight[key] = not (low <= val <= high)
        else:
            highlight[key] = not any(low <= val <= high for (low, high) in limits[key])
    return highlight

@app.route('/monitoring')
def monitoring():
    df, df['Datetime'], today, month, year = loadData()


    # # 공정 진행 상황 (모의 데이터)
    process_status = {
        '산화공정': ['chamber-complete', 'chamber-complete', 'chamber-complete'],
        '포토공정<br>(SoftBake)': ['chamber-complete', 'chamber-complete', 'chamber-complete'],
        '포토공정<br>(Lithography)': ['chamber-active', 'chamber-active', 'chamber-active'],
        '식각공정': ['chamber-idle', 'chamber-idle', 'chamber-idle'],
        '증착공정': ['chamber-idle', 'chamber-idle', 'chamber-idle'],
    }


    # 선택된 route 값 가져오기 (기본값은 2213)
    selected_route = request.args.get('route', default=2213, type=int)

    # unique Route 목록
    all_routes = sorted(df['Route'].dropna().unique().tolist())

    # C 차트 데이터
    c_df = df[df['Route'] == selected_route].copy().reset_index(drop=True)
    c_df['index'] = c_df.index + 1

    c_bar = c_df['Target'].mean()
    UCL = c_bar + 3 * np.sqrt(c_bar)
    LCL = max(0, c_bar - 3 * np.sqrt(c_bar))

    c_df['OutOfControl'] = (c_df['Target'] > UCL) | (c_df['Target'] < LCL)

    c_chart_data = {
        'index': c_df['index'].tolist(),
        'target': c_df['Target'].tolist(),
        'ucl': [round(UCL, 2)] * len(c_df),
        'lcl': [round(LCL, 2)] * len(c_df),
        'center': [round(c_bar, 2)] * len(c_df),
        'out_of_control': c_df['OutOfControl'].tolist(),
    }
    # 최근 공정 테이블 데이터
    latest = df[df['Datetime'] == df['Datetime'].max()].copy()
    process_table = []
    for _, row in latest.iterrows():
        highlight = check_limit(row)
        row_dict = {
            'Datetime': row['Datetime'].strftime('%Y-%m-%d'),
            'Wafer_Num': row['Wafer_Num'],
            'Lot_Num': row['Lot_Num'],
            'Route': row['Route'],
            'Target': row['Target'],
            'is_defect': row.get('is_defect', 0),
            'params': {
                key: {
                    'value': row[key],
                    'highlight': highlight.get(key, False)
                }
                for key in highlight
            }
        }
        process_table.append(row_dict)


    return render_template(
        'monitoring.html',
        process_status=process_status,
        process_table=process_table,
        c_chart_data=c_chart_data,
        all_routes=all_routes,
        current_route=selected_route,
    )

@app.route('/schedule')
def schedule():

    df, df['Datetime'], today, month, year = loadData()

    # 불량률 계산용 route 정제
    df['Route'] = df['Route'].astype(str)
    df = df[df['Route'].str.len() == 4].copy()
    df['Route5'] = df['Route'] + df['Route'].str[-1]

    # Route별 불량률 계산
    df['chip_count'] = 520
    route_group = df.groupby('Route5').agg(
        WaferCount=('Route5', 'count'),
        DefectChips=('Target', 'sum'),
        Chamber_Num_oxi=('Chamber_Num_oxi', 'first'),
        Chamber_Num_sb=('Chamber_Num_sb', 'first'),
        Chamber_Num_litho=('Chamber_Num_litho', 'first'),
        Chamber_Num_etch=('Chamber_Num_etch', 'first'),
        Chamber_Num_ion=('Chamber_Num_ion', 'first'),
    ).reset_index()
    route_group['DefectRate'] = (route_group['DefectChips'] / (route_group['WaferCount'] * 520)) * 100
    route_group = route_group.sort_values(by='DefectRate').reset_index(drop=True)

    # 조합 216개 구하기: 각 자리 1,2,3이 겹치지 않도록
    valid_routes = route_group['Route5'].tolist()
    combinations = list(itertools.combinations(valid_routes, 3))

    def is_valid_combo(r1, r2, r3):
        for i in range(4):  # 앞 4자리에서 겹치는 숫자 없어야 함
            if len(set([r1[i], r2[i], r3[i]])) != 3:
                return False
        return True

    valid_combos = [combo for combo in combinations if is_valid_combo(*combo)]

    # 평균 불량률 계산
    combo_records = []
    for r1, r2, r3 in valid_combos:
        rates = []
        loads = []
        for r in [r1, r2, r3]:
            row = route_group[route_group['Route5'] == r]
            if not row.empty:
                rates.append(row['DefectRate'].values[0])
                loads.append(row['WaferCount'].values[0])
        if len(rates) == 3:
            avg_defect = sum(rates) / 3
            load_flags = ''.join(['1' if l > 226 else '0' for l in loads])
        combo_records.append({
                '투입경로': f"[{r1}, {r2}, {r3}]",
                '불량률(%)': round(avg_defect, 2),
                '부하상태': load_flags
            })

    combo_df = pd.DataFrame(combo_records).sort_values(by='불량률(%)').reset_index(drop=True)
    combo_df['Rank'] = combo_df.index + 1

    return render_template(
    'schedule.html',
       route_defect_table=route_group,
       combo_defect_table=combo_df,
    )

if __name__ == '__main__':
    app.run(debug=True)