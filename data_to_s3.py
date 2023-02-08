import base64
import json, jsonpath, pymysql
import logging
import sys
from datetime import datetime
import boto3
import pandas as pd
import requests

import configparser

# config.ini 불러오기
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

client_id = config['API']['client_id']
client_secret = config['API']['client_secret']

try:
    con = pymysql.connect(host=config['DB']['host'],
                          user=config['DB']['user'],
                          passwd=config['DB']['password'],
                          db=config['DB']['database'],
                          port=int(config['DB']['port']),
                          use_unicode=True,
                          charset='utf8')
    cur = con.cursor()
except:
    logging.error("mysql connection error")
    sys.exit(1)

def get_token(client_id, client_secret):
    endpoint = "https://accounts.spotify.com/api/token"

    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {"Authorization": "Basic {}".format(encoded)}
    payload = {"grant_type": "client_credentials"}

    response = requests.post(endpoint, data=payload, headers=headers)
    access_token = json.loads(response.text)['access_token']

    headers = {"Authorization": "Bearer {}".format(access_token)}

    return headers


def invoke_lambda(fxn_name, payload, invocation_type='Event'):
    # invocation_type -> 'Event': 비동기, 'RequestResponse': 동기
    lambda_client = boto3.client('lambda', region_name="us-east-1")
    invoke_response = lambda_client.invoke(
        FunctionName=fxn_name,
        InvocationType=invocation_type,
        Payload=json.dumps(payload)
    )

    return invoke_response


def main():
    cur.execute("SELECT artist_id, artist_name FROM artists")

    top_track_keys = {
        "track_id": "id",
        "track_name": "name",
        "popularity": "popularity",
        "album_name": "album.name",
        "image_url": "album.images[1].url"
    }

    top_tracks = []
    audio_features = []

    # Top Track Data Flatten
    for (a_id, a_name) in cur.fetchall():
        endpoint = "https://api.spotify.com/v1/artists/{}/top-tracks".format(a_id)
        headers = get_token(client_id, client_secret)
        query_params = {'market': 'KR'}
        response = requests.get(endpoint, params=query_params, headers=headers)
        data = json.loads(response.text)

        # # Update DynamodDB Data
        # response = invoke_lambda('dynamo-function', payload={
        #     'data': data
        # })

        for track in data['tracks']:
            n_track = {}
            for k, v in top_track_keys.items():
                value = jsonpath.jsonpath(track, v)
                if type(value) == bool:
                    continue
                n_track.update({k: value[0]})
                n_track.update({'artist_id': a_id})
                n_track.update({'artist_name': a_name})

            top_tracks.append(n_track)

    track_ids = [track['track_id'] for track in top_tracks]
    print(top_tracks)
    # audio Feature API 호출을 track id 100개씩 묶어 처리
    list_track_id_binds = [track_ids[i:i + 100] for i in range(0, len(track_ids), 100)]

    for track_id_bind in list_track_id_binds:
        ids = ','.join(track_id_bind)
        endpoint = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)
        headers = get_token(client_id, client_secret)
        response = requests.get(endpoint, headers=headers)
        data = json.loads(response.text)

        audio_features.extend(data['audio_features'])

    # to DataFrame -> parquet File
    top_tracks = pd.DataFrame(top_tracks)
    audio_features = pd.DataFrame(audio_features)

    top_tracks.to_parquet('top-tracks.parquet', engine="pyarrow", compression="snappy")
    audio_features.to_parquet('audio-features.parquet', engine="pyarrow", compression="snappy")

    # to s3
    s3 = boto3.resource('s3')
    time = datetime.utcnow().strftime("%Y-%m-%d")  # 2023-01-17
    bucket = s3.Object(config['AWS']['bucket_name'], 'top-tracks/dt={}/top_tracks.parquet'.format(time))
    data = open('top-tracks.parquet', 'rb')
    bucket.put(Body=data)

    s3 = boto3.resource('s3')
    time = datetime.utcnow().strftime("%Y-%m-%d")
    bucket = s3.Object(config['AWS']['bucket_name'], 'audio-features/dt={}/audio_features.parquet'.format(time))
    data = open('audio-features.parquet', 'rb')
    bucket.put(Body=data)


if __name__ == '__main__':
    main()
