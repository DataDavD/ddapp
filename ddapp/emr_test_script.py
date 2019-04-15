from pyspark.sql import SparkSession, Row
import boto3
import json
# from aws_cred import ACCESS_KEY, SECRET_KEY

# connect to EMR client
emr_client = boto3.client('emr', region_name='us-east-1')

# upload file to an S3 bucket
s3 = boto3.resource('s3')

response = emr_client.run_job_flow(
    Name="ddapi EMR Cluster",
    ReleaseLabel='emr-5.23.0',
    Instances={
        'MasterInstanceType': 'm1.xlarge',
        'SlaveInstanceType': 'm1.xlarge',
        'InstanceCount': 2,
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    Applications=[
        {'Name': 'Hadoop'},
        {'Name': 'Spark'}
    ],
    BootstrapActions=[
        {
            'Name': 'bootstrap test',
            'ScriptBootstrapAction': {
                'Path': 's3://ddapi.data/ddapp_emr_bootstrap.sh',
            }
        },
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
)

job_flow_id = response['JobFlowId']
print(job_flow_id)

step_response = emr_client.add_job_flow_steps(
        JobFlowId=job_flow_id,
        Steps=[{
            'Name': 'Spark Application',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
               'Jar': 'command-runner.jar',
               'Args': ["spark-submit", '/home/hadoop/emr_test.py']
            }
        }]
    )

steps_id = step_response['StepIds']
print("Step IDs:", steps_id)



spark = SparkSession \
    .builder \
    .appName("DDapp_model_updt") \
    .getOrCreate()

s3_resource = boto3.resource('s3')
obj = s3_resource.Object('ddapi.data', 'modelDataFrame.json')
data = obj.get()['Body'].read().decode()

data = json.loads(data)

df = spark.createDataFrame(Row(**x) for x in data)
df.show()

# df.write.csv('s3a://ddapi.data/dftest.csv')
