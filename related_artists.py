import sys, os, logging, pickle
import boto3  # athena 필요
import time, math  # time.sleep 사용
from datetime import datetime
import pymysql
import configparser

# config.ini 불러오기
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')


try:
    conn = pymysql.connect(host=config['DB']['host'],
                          user=config['DB']['user'],
                          passwd=config['DB']['password'],
                          db=config['DB']['database'],
                          port=int(config['DB']['port']),
                          use_unicode=True,
                          charset='utf8')# 한글처리 (charset = 'utf8')
    cursor = conn.cursor()
except:
    logging.error("connection error")
    sys.exit(1)



athena = boto3.client('athena')


# 정규화 계산 함수
def normalize(x, x_min, x_max):
    normalized = (x - x_min) / (x_max - x_min)
    return normalized


# Athena에서 사용할 데이터 configure 하는 함수
def query_athena(query, athena):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'related_artists'
        },
        ResultConfiguration={
            # 쿼리 결과 저장하는 위치 지정
            'OutputLocation': 's3://spotify-music-data-bucket/related_artists',
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )

    return response


def get_query_result(query_id, athena):
    response = athena.get_query_execution(
        QueryExecutionId=str(query_id)
    )

    # 쿼리가 완료될 때까지 충분한 시간 대기
    while response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
        if response['QueryExecution']['Status']['State'] == 'FAILED':
            logging.error('QUERY FAILED')
            break
        time.sleep(5)  # 데이터의 양을 보면, Athena에서 처리 시간을 통해 어느 정도 걸리는지 알 수 있음 -> 5초
        response = athena.get_query_execution(
            QueryExecutionId=str(query_id)
        )

    response = athena.get_query_results(
        QueryExecutionId=str(query_id),
        MaxResults=1000  # Athena는 MaxResults가 1000
    )

    return response


# 쿼리 결과 데이터 전처리
def process_data(results):
    data = results['ResultSet']
    columns = [col['VarCharValue'] for col in data['Rows'][0]['Data']]

    listed_results = []
    for row in data['Rows'][1:]:
        values = []
        for col in row['Data']:
            try:
                values.append(col['VarCharValue'])  
            except: 
                values.append('')
        listed_results.append(dict(zip(columns, values)))

    return listed_results


# mysql의 table에 데이터 insert 하는 함수
def insert_row(cursor, data, table):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())

    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    # 기본적으로 insert 하되, 키가 같으면 update
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (
    table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values()) * 2)  # *2: values()와 on duplicate key update에 값이 중복되어 들어가기 때문에, 2번 사용


############################

def main():
    start = time.time()

    # 1. top_tracks 데이터 업데이트
    query = """
        create external table if not exists top_tracks(
        track_id string,
        artist_id string,
        artist_name string,
        track_name string,
        album_name string,
        popularity int,
        image_url string
        ) partitioned by (dt string)
        stored as parquet location 's3://{}/top-tracks' tblproperties("parquet.compress" = "snappy")
    """.format(config['AWS']['bucket_name'])
    r = query_athena(query, athena)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table top_tracks'
        r = query_athena(query, athena)
        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            result = get_query_result(r['QueryExecutionId'], athena)
            # print(result) # 파티션 생성 결과
            print('top_tracks partition update!')  # 신규 파티션 생성

    # 2. audio_features 데이터 업데이트
    query = """
        create external table if not exists audio_features(
        duration_ms int,
        key int,
        mode int,
        time_signature int,
        acousticness double,
        danceability double,
        energy double,
        instrumentalness double,
        liveness double,
        loudness double,
        speechiness double,
        valence double,
        tempo double,
        id string
        ) partitioned by (dt string)
        stored as parquet location 's3://{}/audio-features' tblproperties("parquet.compress" = "snappy")
    """.format(config['AWS']['bucket_name'])
    r = query_athena(query, athena)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table audio_features'
        r = query_athena(query, athena)
        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            result = get_query_result(r['QueryExecutionId'], athena)

    # 3. 아티스트별 평균 수치 계산
    query = """
        SELECT
            artist_id,
            avg(danceability) as danceability,
            avg(energy) as energy,
            avg(loudness) as loudness,
            avg(speechiness) as speechiness,
            avg(acousticness) as acousticness,
            avg(instrumentalness) as instrumentalness
        FROM
            top_tracks t1
        JOIN
            audio_features t2 on t2.id = t1.track_id
        WHERE
            t1.dt = (select max(dt) from top_tracks)
            and t2.dt = (select max(dt) from audio_features)
        GROUP BY
            t1.artist_id
    """

    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena)
    artists = process_data(results)

    # # 정규화 위해 수치별 최대, 최소값 계산. 가장 최근 날짜 데이터 사용
    query = """
        SELECT
            MIN(danceability) AS danceability_min,
            MAX(danceability) AS danceability_max,
            MIN(energy) AS energy_min,
            MAX(energy) AS energy_max,
            MIN(loudness) AS loudness_min,
            MAX(loudness) AS loudness_max,
            MIN(speechiness) AS speechiness_min,
            MAX(speechiness) AS speechiness_max,
            ROUND(MIN(acousticness),4) AS acousticness_min,
            MAX(acousticness) AS acousticness_max,
            MIN(instrumentalness) AS instrumentalness_min,
            MAX(instrumentalness) AS instrumentalness_max
        FROM
            audio_features
        WHERE
            dt = (select max(dt) from audio_features)
    """

    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena)
    avgs = process_data(results)[0]

    metrics = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness']

    for i in range(len(artists)):
        data = []  # 일단 다 넣고, 정렬해서 최소 거리인 5개를 sql에 넣기

        others = artists.copy()  # temp: 자기 자신 뺀 것.
        mine = others.pop(i)  # mine: 자기 자신.
        for other in others:
            dist = 0
            for m in metrics:
                # mine과 other 간 거리 계산
                x = float(mine[m])
                x_norm = normalize(x, float(avgs[m + '_min']), float(avgs[m + '_max']))
                y = float(other[m])
                y_norm = normalize(y, float(avgs[m + '_min']), float(avgs[m + '_max']))
                dist += math.sqrt((x_norm - y_norm) ** 2)

            if dist != 0:
                temp = {
                    'artist_id': mine['artist_id'],
                    'related_artist_id': other['artist_id'],
                    'distance': dist
                }
                data.append(temp)

        # 아티스트별로 가까운 5개만 MySQL에 삽입
        # 날짜는 삽입 시점의 timestamp로 넣도록 테이블에서 설정해 놓았으므로, 신경 쓰지 않아도 됨
        data = sorted(data, key=lambda x: x['distance'])[:5]
        for d in data:
            insert_row(cursor, d, 'related_artists')

    conn.commit()
    cursor.close()


if __name__ == "__main__":
    main()