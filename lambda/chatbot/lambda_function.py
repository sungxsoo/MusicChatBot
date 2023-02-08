import logging
import time

import requests
import base64
import json
import boto3

import logging
import sys

import pymysql
import configparser

from urllib import parse
from boto3.dynamodb.conditions import Key

# config.ini 불러 오기
config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8')

# 유튜브 검색 결과 링크 (각 트랙 별로 parse해서 사용)
base_url = "https://www.youtube.com/results?"
client_id = config['API']['client_id']
client_secret = config['API']['client_secret']

tracks = ()

# DynomoDB 연결: AWS CLI Config 정보 바탕으로 boto3 사
try:
    dynamodb = boto3.resource("dynamodb")
except:
    logging.error('error: cannot connect to dynamodb')
    sys.exit(1)


# RDS (mysql) 연결
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


# API 헤더에 들어가는 Token
def get_token(client_id, client_secret):
    endpoint = "https://accounts.spotify.com/api/token"

    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {"Authorization": "Basic {}".format(encoded)}
    payload = {"grant_type": "client_credentials"}

    response = requests.post(endpoint, data=payload, headers=headers)
    access_token = json.loads(response.text)['access_token']

    headers = {"Authorization": "Bearer {}".format(access_token)}

    return headers


# Lambda Function 비동기 호출: DynamodDB로 데이터 저장 과정은 다른 람다 함수에서 처리
def invoke_lambda(funcntion_name, payload, invocation_type='Event'):
    lambda_client = boto3.client('lambda')
    invoke_response = lambda_client.invoke(
        FunctionName=funcntion_name,
        InvocationType=invocation_type,
        Payload=json.dumps(payload)
    )

    return invoke_response


# DB 에서 Top_Track 데이터 가져 오는 함수
def get_top_track(artist_id, artist_name):
    table = dynamodb.Table('artist_tracks')
    # 아티스트의 id를 기반으로 track 데이터를 읽어온다.
    result = table.query(
        KeyConditionExpression=Key('artist_id').eq(artist_id)
    )
    result['Items'].sort(key=lambda x: x['popularity'], reverse=True)

    items = []

    for ele in result['Items'][:3]:
        track_name = ele['track_name']
        query = {
            'search_query': '{} {}'.format(artist_name, track_name)
        }

        youtube_url = base_url + parse.urlencode(query, encoding='UTF-8', doseq=True)

        # ListCard 형태에 맞게 리턴
        temp_dic = {
            "title": track_name,
            "description": ele['album_name'],
            "imageUrl": ele['image_url'],
            "link": {
                "web": youtube_url
            }
        }

        items.append(temp_dic)

    return items


# API 로 Top_Track 데이터 검색 하는 함수
def search_top_track(a_id, a_name):
    url = "https://api.spotify.com/v1/artists/{}/top-tracks".format(a_id)
    headers = get_token(client_id, client_secret)
    query_params = {'market': 'KR'}
    result = requests.get(url, params=query_params, headers=headers)

    raw = json.loads(result.text)

    global tracks
    tracks = raw

    items = []

    for ele in raw['tracks'][:3]:
        name = ele['name']
        query = {
            'search_query': '{} {}'.format(a_name, name)
        }

        youtube_url = base_url + parse.urlencode(query, encoding='UTF-8', doseq=True)

        # ListCard 형태에 맞게 리턴
        temp_dic = {
            "title": name,
            "description": ele['album']['name'],
            "imageUrl": ele['album']['images'][1]['url'],
            # images는 같은 앨범 이미지에 대해서 크기별로 넣어 놓은 것. 1이 적당한 사이즈(300x300)라 고름
            "link": {
                "web": youtube_url
            }
        }
        items.append(temp_dic)

    return items;


# API 로 Artist 데이터 검색 하는 함수
def search_artist(artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_token(client_id, client_secret)
    query_params = {'q': artist_name, 'type': 'artist', 'limit': 1}

    result = requests.get(url, params=query_params, headers=headers)

    if result.status_code != 200:
        logging.error(json.loads(result.text))
        # 너무 많은 데이터
        if result.status_code == 429:
            retry_after = json.loads(result.headers)['retry-After']
            time.sleep(int(retry_after))
            result = requests.get(url, params=query_params, headers=headers)
        # token expired
        elif result.status_code == 401:
            headers = get_token(client_id, client_secret)
            result = requests.get(url, params=query_params, headers=headers)
        # other errors
        else:
            logging.error(json.loads(result.text))

    data = json.loads(result.text)
    artist_data = data['artists']['items'][0]

    artist = {
        'artist_id': artist_data['id'],
        'artist_name': artist_data['name'],
        'followers': artist_data['followers']['total'],
        'popularity': artist_data['popularity'],
        'artist_url': artist_data['external_urls']['spotify'],
        'image_url': artist_data['images'][0]['url']
    }

    return artist


# DB에 Artist 데이터 저장 하는 함수
def insert_artist_db(artist):
    insert_query = 'insert into artists values (%s,%s,%s,%s,%s,%s)'
    cur.execute(insert_query, (
        artist['artist_id'], artist['artist_name'], artist['followers'], artist['popularity'], artist['artist_url'],
        artist['image_url']))
    con.commit()


# DB에서 Artist 데이터 가져오는 함수
def get_artist(artist_id):
    try:
        sql = "select * from artists where artist_id = '{}'".format(artist_id)
        cur.execute(sql)
        res = cur.fetchall()[0]
        cols = [ele[0] for ele in cur.description]

        return {k: v for k, v in zip(cols, res)}
    except:
        return


# DB에서 Artist 데이터 가져오는 함수
def get_artist_by_name(artist_name):
    try:
        sql = "select * from artists where artist_name = '{}'".format(artist_name)
        print(sql)
        cur.execute(sql)
        res = cur.fetchall()[0]
        cols = [ele[0] for ele in cur.description]

        return {k: v for k, v in zip(cols, res)}
    except:
        return


# DB에서 Related_Artist 데이터 가져오는 함수
def get_related_artists_db(artist_id):
    try:
        sql = "select related_artist_id from spotify_chatbo_db.related_artists where artist_id = '{}' order by distance limit 3".format(
            artist_id)
        cur.execute(sql)
        res = [ele[0] for ele in cur.fetchall()]  # id 목록만 리턴
        return res
    except:
        return


# 카카오톡 응답 관련 함수
def item_card(title, imageUrl, popularity, followers, externalUrl):
    return {
        "itemCard": {
            "thumbnail": {
                "imageUrl": imageUrl,
                "width": 800,
                "height": 800
            },
            "profile": {
                "title": title,
                "imageUrl": imageUrl

            },
            "itemList": [
                {
                    "title": "Popularity",
                    "description": popularity
                },
                {
                    "title": "Followers",
                    "description": followers
                }
            ],
            "buttons": [
                {
                    "label": "Spotify 검색",
                    "action": "webLink",
                    "webLinkUrl": externalUrl
                }
            ]
        }
    }


# SimpleText 메시지
def simple_text(msg):
    return {
        "simpleText": {
            "text": msg
        }
    }


# ListCard 메시지
def list_card(title, imageUrl, items, externalUrl):
    return {
        "listCard": {
            "header": {
                "title": title,
                "imageUrl": imageUrl
            },
            "items": items,
            "buttons": [
                {
                    "label": "다른 노래도 보기",
                    "action": "webLink",
                    "webLinkUrl": externalUrl
                }
            ]
        }
    }


# Carousel (여러 장의 카드 메시지)
# carousel의 type은 필요하면 수정할 수 있도록, 기본값(현재 listCard)을 넣음
def carousel(items, card_type="listCard"):
    return {
        "carousel": {
            "type": card_type,
            "items": items
        }
    }


# 챗봇 메시지
def message(outputs):
    return {
        "version": "2.0",
        "template": {
            "outputs": outputs  # 여기에 메시지 카드들이 들어감(list로)
        }
    }


# 최종 response
def response(result):
    return {
        'statusCode': 200,
        'body': json.dumps(result),
        'headers': {
            'Access-Control-Allow-Origin': '*',
        }
    }


####################################


def lambda_handler(event, context):
    request_body = json.loads(event['body'])
    params = request_body['action']['params']

    artist_name = request_body['userRequest']['utterance'].rstrip("\n")

    artist = search_artist(artist_name)

    # 검색 결과가 없을 경우 -> 한영 변환, 띄어쓰기 조정 처리 후 다시 검색
    if not artist:
        return

    # 쿼리를 통해 기존 DB에 데이터 유무 파악
    artist_db_data = get_artist(artist['artist_id'])

    # CASE 1: 입력된 아티스트 데이터가 DB에 없음 (새로운 데이터 insert)
    if not artist_db_data:
        # 메세지 큐
        message_queue = []

        # 아티스트 데이터 Insert
        insert_artist_db(artist)

        # 트랙 데이터 검색 + Insert
        temp_top_tracks = search_top_track(artist['artist_id'], artist['artist_name'])

        # DynamoDB 저장 람다 호
        resp = invoke_lambda('dynamo-function', payload={
            'data': tracks
        })

        youtube_url = 'https://www.youtube.com/results?search_query={}'.format(artist['artist_name'].replace(' ', '+'))

        # 메세지 작성
        card_message = list_card(artist['artist_name'], artist['image_url'], temp_top_tracks, youtube_url)
        text_message = simple_text("{}의 노래를 들어보세요.".format(artist['artist_name']))

        message_queue.append(text_message)
        message_queue.append(card_message)

        result = message(message_queue)
        return response(result)

    # CASE 2: 입력된 아티스트가 DB에 있음 (artist_db_data로 처리)
    youtube_url = 'https://www.youtube.com/results?search_query={}'.format(
        artist_db_data['artist_name'].replace(' ', '+'))

    message_queue = []

    # top_track 검색
    top_tracks = get_top_track(artist_db_data['artist_id'], artist_db_data['artist_name'])

    # 메세지 작성
    card_message = list_card(artist_db_data['artist_name'], artist_db_data['image_url'], top_tracks, youtube_url)

    item_message = item_card(artist_db_data['artist_name'], artist_db_data['image_url'], artist_db_data['popularity'],
                             artist_db_data['followers'], artist_db_data['artist_url'])

    related_artists = get_related_artists_db(artist_db_data['artist_id'])

    # CASE 2-1: 입력된 아티스트의 관련 아티스트도 DB에 있음
    if related_artists:

        list_card_message = []

        list_card_message.append(card_message['listCard'])

        for related_artist in related_artists:
            related_artist_data = get_artist(related_artist)

            rel_artist_id = related_artist_data['artist_id']
            rel_artist_name = related_artist_data['artist_name']
            rel_image_url = related_artist_data['image_url']
            rel_top_tracks = get_top_track(rel_artist_id, rel_artist_name)
            rel_youtube_url = 'https://www.youtube.com/results?search_query={}'.format(
                rel_artist_name.replace(' ', '+'))

            rel_card_message = list_card(rel_artist_name, rel_image_url, rel_top_tracks, rel_youtube_url)['listCard']
            list_card_message.append(rel_card_message)

        text_message = simple_text("{}와 연관 아티스트 노래를 들어보세요!".format(artist_db_data['artist_name']))

        message_queue.append(text_message)
        message_queue.append(item_message)
        message_queue.append(carousel(list_card_message))

    # CASE 2-1: 입력된 아티스트의 관련 아티스트는 DB에 없음
    else:
        text_message = simple_text("{}의 노래를 들어보세요.".format(artist_db_data['artist_name']))
        message_queue.append(text_message)
        message_queue.append(item_message)
        message_queue.append(card_message)

    result = message(message_queue)
    return response(result)
