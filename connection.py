from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

class Connection:
    
    def os_connection(self):
        
        host = 'search-twitter-7hgtnq2vyvlq53ckdx2l7nel4y.aos.us-east-2.on.aws'
        region = 'us-east-2'

        service = 'es'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(region = region, service = service, session_token = credentials.token, refreshable_credentials = credentials)

        os = OpenSearch(
            hosts = [{'host': host, 'port': 443}],
            http_auth = awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection)

        return os
    
    def rds_connection(self):
        
        db_username = 'twitterrelationaldb'
        db_password = 'DbmsRutgers123'
        db_endpoint = 'twitter-relational-db.c1eiwiissq2z.us-east-2.rds.amazonaws.com'
        db_name = 'twitter-relational-db'

        rds = pymysql.connect(host=db_endpoint, user=db_username, password=db_password, database=db_name)
