import base64
import json
import logging
import math
import pymysql
import sys
import time
import boto3
import pandas as pd
import pendulum
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import configparser


# skeleton file
kst = pendulum.timezone("Asia/Seoul")

default_args = {
    # dag의 시작 기준
    'start_date': datetime(2023, 1, 1, tzinfo=kst)
}

athena = boto3.client('athena')

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
                          charset='utf8')
    cursor = con.cursor()
except:
    logging.error("mysql connection error")
    sys.exit(1)



def get_token():
    client_id = config['API']['client_id']
    client_secret = config['API']['client_secret']  
    endpoint = "https://accounts.spotify.com/api/token"

    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {"Authorization": "Basic {}".format(encoded)}
    payload = {"grant_type": "client_credentials"}

    response = requests.post(endpoint, data=payload, headers=headers)
    access_token = json.loads(response.text)['access_token']

    headers = {"Authorization": "Bearer {}".format(access_token)}

    return headers


def get_query_result(query_id):
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
    # columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    listed_results = []
    for row in data['Rows'][1:]:  # 행별로 저장
        values = []
        for col in row['Data']:
            try:
                values.append(col['VarCharValue'])  # 각 칼럼의 값들이 {'VarCharValue': value} 형식
            except:  # null일 경우?
                values.append('')
        listed_results.append(dict(zip(columns, values)))

    return listed_results


def normalize(x, x_min, x_max):
    normalized = (x - x_min) / (x_max - x_min)
    return normalized


def insert_row(data, table):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())

    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    # 기본적으로 insert 하되, 키가 같으면 update
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (
        table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values()) * 2)  # *2: values()와 on duplicate key update에 값이 중복되어 들어가기 때문에, 2번 사용

# top_tracks 데이터 로딩
def load_top_tracks():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('artist_tracks')
    response = table.scan(
        ProjectionExpression='artist_id, track_id',
    )
    top_tracks = response['Items']
    top_tracks = pd.DataFrame(top_tracks)
    top_tracks.to_parquet('/tmp/top-tracks.parquet', engine="pyarrow", compression="snappy")


# audio_feature 데이터 로딩
def load_audio_features():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('artist_tracks')
    response = table.scan()
    track_data = response['Items']
    track_ids = [track['track_id'] for track in track_data]

    audio_features = []
    list_track_id_binds = [track_ids[i:i + 100] for i in range(0, len(track_ids), 100)]

    for track_id_bind in list_track_id_binds:
        ids = ','.join(track_id_bind)
        endpoint = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)
        headers = get_token()
        response = requests.get(endpoint, headers=headers)
        data = json.loads(response.text)

        audio_features.extend(data['audio_features'])
        audio_features = pd.DataFrame(audio_features)
        audio_features.to_parquet('/tmp/audio-features.parquet', engine="pyarrow", compression="snappy")



def upload_to_s3():
    s3 = boto3.resource('s3')
    date_time = datetime.utcnow().strftime("%Y-%m-%d")  
    bucket = s3.Object(config['AWS']['bucket_name'], 'top-tracks/dt={}/top_tracks.parquet'.format(time))
    data = open('/tmp/top-tracks.parquet', 'rb')
    bucket.put(Body=data)

    s3 = boto3.resource('s3')
    date_time = datetime.utcnow().strftime("%Y-%m-%d")
    bucket = s3.Object(config['AWS']['bucket_name'], 'audio-features/dt={}/audio_features.parquet'.format(time))
    data = open('/tmp/audio-features.parquet', 'rb')
    bucket.put(Body=data)


def query_athena(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'related_artists'
        },
        ResultConfiguration={
            # 쿼리 결과 저장하는 위치 지정
            'OutputLocation': 's3://{}/related_artists'.format(config['AWS']['bucket_name']),
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )

    return response


def create_top_track_athena_table():
    query = """
        create external table if not exists top_tracks(
        artist_id string,
        track_id string
        ) partitioned by (dt string)
        stored as parquet location 's3://{}/top-tracks' tblproperties("parquet.compress" = "snappy")
    """.format(config['AWS']['bucket_name'])
    r = query_athena(query)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table top_tracks'
        r = query_athena(query)
        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('top_tracks partition update!')


def create_audio_features_athena_table():
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
    r = query_athena(query)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        query = 'msck repair table audio_features'
        r = query_athena(query)
        if r['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('audio_features partition update!')


def query_data():
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

    r = query_athena(query)
    results = get_query_result(r['QueryExecutionId'])
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

    r = query_athena(query)
    results = get_query_result(r['QueryExecutionId'])
    avgs = process_data(results)[0]

    metrics = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness']

    related_data = []

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

        related_data.append(data)

    return related_data


def store_data(**context):
    related_data = context['task_instance'].xcom_pull(task_ids='query_data')
    for data in related_data:
        for row in data:
            insert_row(row, 'related_artists')
    conn.commit()
    cursor.close()


with DAG(dag_id='data_to_s3_pipeline',
         # 주기
         schedule_interval='@daily',
         default_args=default_args,
         tags=['related_artist'],
         catchup=False) as dag:
    start_pipeline = EmptyOperator(
        task_id='start_pipeline'
    )

    load_audio_features = PythonOperator(
        task_id='load_audio_features',
        python_callable=load_audio_features
    )

    load_top_tracks = PythonOperator(
        task_id='load_top_tracks',
        python_callable=load_top_tracks
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    top_track_athena_table = PythonOperator(
        task_id='top_track_athena_table',
        python_callable=create_top_track_athena_table
    )

    audio_features_athena_table = PythonOperator(
        task_id='audio_features_athena_table',
        python_callable=create_audio_features_athena_table
    )

    query_data = PythonOperator(
        task_id='query_data',
        python_callable=query_data
    )

    store_data = PythonOperator(
        task_id='store_data',
        python_callable=store_data
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline'
    )

start_pipeline >> [load_audio_features, load_top_tracks]

[load_audio_features, load_top_tracks] >> upload_to_s3 >> [top_track_athena_table, audio_features_athena_table]

[top_track_athena_table, audio_features_athena_table] >> query_data >> store_data >> end_pipeline
