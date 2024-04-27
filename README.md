# TweetSearchApp

Date - April 27th 2024

To run the code you will need to create an root user account on the AWS Management Console.

Step 1: Create an Opensearch Domain - https://docs.aws.amazon.com/opensearch-service/latest/developerguide/createupdatedomains.html
Step 2: Create an Sagemaker Instance - https://docs.aws.amazon.com/sagemaker/latest/dg/howitworks-create-ws.html
Step 3: Create an RDS database - https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html
Step 4: Run the following command in the terminal - 
        -> pip install -r requirements.txt
        -> python connection.py
        -> python support_functions.py
        -> python BackendDataCuration.py
        -> python Cache.py
        -> python SearchApp.py
Step 5: Run the notebook - Frontend.ipynb to see a demo of the appplication.

Handling Permissions and IAM roles - https://www.youtube.com/watch?v=KeUBwm-aalU

Repo Shareable Links - 
1. https://twitter-instance-2.notebook.us-east-2.sagemaker.aws/lab/tree/TweetSearchApp
2. https://github.com/adishofwhat/TweetSearchApp

Note: Make sure to replace the endpoints/ARNs of the Opensearch and RDS with your endpoints/ARNs

References:
1. https://www.youtube.com/watch?v=wWKiVmgj-aU
2. https://www.youtube.com/watch?v=1xaJODdnnOk
3. https://www.youtube.com/watch?v=1xaJODdnnOk



Team members:

Adish Golechha (ag2384)

Fnu Vasureddy (fv121)

Pradhyumna Kanth Goud Kasula (pk732)

Shreyash Kalal (ssk241)

