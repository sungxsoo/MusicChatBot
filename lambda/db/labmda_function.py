import boto3
import json

import jsonpath

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    table = dynamodb.Table('artist_tracks')

    data = event['data']

    top_track_keys = {
        "track_id": "id",
        "track_name": "name",
        "popularity": "popularity",
        "external_url": "external_url.spotify",
        "album_name": "album.name",
        "image_url": "album.images[1].url"
    }

    with table.batch_writer() as batch:
        for track in data['tracks']:

            temp = {
                'artist_id': track['artists'][0]['id'],
                'artist_name': track['artists'][0]['name']
            }
            for k, v in top_track_keys.items():
                value = jsonpath.jsonpath(track, v)
                if type(value) == bool:
                    continue
                temp.update({k: value[0]})

            response = table.put_item(
                Item=temp
            )

    return response