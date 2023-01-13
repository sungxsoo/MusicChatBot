import boto3
import json

dynamodb = boto3.resource("dynamodb")

def lambda_handler(event, context):
    table = dynamodb.Table('top_track')

    data = event['data']

    for ele in data['tracks'][:3]:
        id = ele['id']
        track_name = ele['name']
        album_data = ele['album']
        popularity =ele['popularity']
        artist_id = ele['artists'][0]['id']
        artist_name = ele['artists'][0]['name']

        response = table.put_item(
            Item = {
                'id' : id,
                'album_data': album_data,
                'artist_id' : artist_id,
                'artist_name': artist_name,
                'track_name': track_name,
                'popularity': popularity,
            }
        )

    return response