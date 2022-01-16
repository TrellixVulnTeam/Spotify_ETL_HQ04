import boto3

s3 = boto3.client('s3')
s3.upload_file('my_played_tracks.sqlite','spotify-jakub','my_played_tracks.sqlite')
