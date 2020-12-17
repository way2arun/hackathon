from __future__ import print_function
from pprint import pprint
import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
# from requests_aws4auth import AWS4Auth
import lambda_function_common_def

#import urllib
import json
#import requests

metadata_store = lambda_function_common_def.metadata_store_name



region = 'us-east-1'  # For example, us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

s3 = boto3.client('s3')

print('Loading function')



indexDoc = {
    "mappings" : {
        "properties" : {
          #"created_date" : {"type" : "date","format" : "dateOptionalTime"},
          "content_type" : {"type" : "text"},
          "content_length" : {"type" : "long"},
          "aws_region" : {"type" : "keyword"},
          # location 1dx-d-z1-sc-process-data
          # 1dx - indicates a one fdigital platform dataset, if this is different we will ignore
          # d - d: development t:test p:production
          # z1 - z1: zone 1 z2: zone 2 ... valid values 1 - 5
          # sc: site_buisness unit sc = seccam
          # process-data - dataset name
          "data_set" : {"type" : "keyword"},
          # sub_dataset -> this can be a table from a tablebase or data domain, is equivilant to the first directory in the repfix or key
          "sub_dataset" : {"type" : "keyword"},
          # data_sources is for lineage a list of datasourcs that went into this datasource
          "data_sources" : {"type" : "keyword"},
          "site_business_unit" : {"type" : "keyword"},
          "location" : {"type" : "keyword"},
          "key" : {"type" : "keyword"},
          "bucket" : {"type" : "keyword"},
          "zone" : {"type" : "integer"},
          "environment" : {"type" : "keyword"},
          # buisiness metadata
          "metadata" : {"type" : "nested"},
          "tags" : {"type" : "nested"}
        }
      },
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
      }
    }


def connectES(esEndPoint):
    print ('Connecting to the ES Endpoint {0}'.format(esEndPoint))
    try:
        esClient = Elasticsearch(
            hosts=[{'host': esEndPoint, 'port': 443}],
            # http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        return esClient
    except Exception as E:
        print("Unable to connect to {0}".format(esEndPoint))
        print(E)
        exit(3)

def createIndex(esClient):
    try:
        res = esClient.indices.exists(metadata_store)
        print("index exist: {0}".format(res))
        if res is False:
            esClient.indices.create(metadata_store, body=indexDoc)
        return 1
    except Exception as E:
            print("Unable to Create Index {0}".format(metadata_store))
            print(E)
            exit(4)

def parse_bucket_information(bucket, key):
    response = dict()
    parts = bucket.split('-')
    sub_parts = key.split('/')

    print(parts)

    response["dataset"] = ""
    response["site_business_unit"] = ""
    response["zone"] = ""
    response["environment"] = ""
    response["valid"] = "True"

    # example 1dxp-d-z1-micro-sv-process-data
    if parts[0] == '1dxp' or len(parts) >= 6:
        if parts[1] in ['d', 't', 'p']:
            response["environment"] = parts[1]
        if parts[2] in ['z1', 'z2', 'z3', 'z4', 'z5']:
            # just take the number bc numeric field
            response["zone"] = parts[2][1]

        response["location"] = parts[4]
        response["site_business_unit"] = parts[3]

        for p in parts[4:]:
            response["dataset"] = response["dataset"] + p + '-'

        response["dataset"] = response["dataset"][:-1]
    else:
        # not a ONE Digital platform bucket, ignore
        response["valid"] = "False"

    if len(sub_parts) > 1:
        response["sub_dataset"] = sub_parts[0]
    else:
        response["sub_dataset"] = ''

    response["data_sources"] = ''

    return response

def indexDocElement(esClient, key, bucket, response, tags, aws_region):
    try:
        bucket_info = parse_bucket_information(bucket, key)
        print("KH: {0}".format(response))
        indexObjectKey = key
        indexcreatedDate = response['LastModified']
        indexcontent_length = response['ContentLength']
        indexcontent_type = response['ContentType']
        indexmetadata = response['Metadata']


        if bucket_info['valid'] == "True":
            print("index: {0}".format(bucket_info))
            retval = esClient.index(index=metadata_store, doc_type='_doc', body={
                    #'created_date': indexcreatedDate,
                    'key': indexObjectKey,
                    'content_type': indexcontent_type,
                    'content_length': indexcontent_length,
                    "aws_region": aws_region,
                    'metadata': indexmetadata,
                    "data_set" : bucket_info['dataset'],
                    "data_sources" : bucket_info['data_sources'],
                    "sub_dataset": bucket_info['sub_dataset'],
                    "location" : bucket_info['location'],
                    "site_business_unit" : bucket_info['site_business_unit'],
                    "bucket" : bucket,
                    "zone" : bucket_info['zone'],
                    "environment" : bucket_info['environment'],
                    "tags" : tags
            })
    except Exception as E:
        print("Document not indexed")
        print("Error: ", E)
        exit(5)

def test_function(es):
    document = {
        "title": "Moneyball",
        "director": "Bennett Miller",
        "year": "2011"
    }

    es.index(index="movies", doc_type="_doc", id="5", body=document)
    print(es.get(index="movies", doc_type="_doc", id="5"))


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    esClient = connectES("search-xxxxxxxxxxxxxxxxxxxxx.us-east-1.es.amazonaws.com")
    print("connected to ES: {0}".format(esClient))
    # test_function(esClient)
    createIndex(esClient)

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    aws_region =  event['Records'][0]["awsRegion"]
    print(aws_region)

    unique_search = query_body = {
      "query": {
        "bool": {
          "must": [
          {
            "match": {
              "key": key
            }
          },
          {
            "match": {
              "bucket": bucket
            }
          }
         ]
        }
      }
    }

    # avoid duplicate entires of the same document
    # find existing documents with the same full path, bucket/key
    # and delete those docuemnts before this functions will add it again
    # bear in mind the _id will change
    result = esClient.search(index=metadata_store, body=unique_search)
    duplicates = result['hits']['total']['value']
    if duplicates > 0:
        for i in range(0,duplicates):
            print("KH delete docuemnt with id: {0}".format(result['hits']['hits'][i]['_id']))
            esClient.delete(index=metadata_store,doc_type="_doc",id=result['hits']['hits'][i]['_id'])

    try:
        print("Get the object from s3 {0}/{1}".format(bucket, key))

        response = s3.get_object(Bucket=bucket, Key=key)
        try:
            tags = s3.get_bucket_tagging(Bucket=bucket)['TagSet']
        except:
            tags = ""

        print("Index File in Metadata-store")
        indexDocElement(esClient, key, bucket, response, tags,aws_region)
        print("Done Index File in Metadata-store")
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
