from pyspark.sql import SparkSession, Row
import boto3
import json
# from aws_cred import ACCESS_KEY, SECRET_KEY

# connect to EMR client
emr_client = boto3.client('emr', region_name='us-east-1')

# upload file to an S3 bucket
s3 = boto3.resource('s3')

# need to refactor formatting of emr job calls below
response = emr_client.run_job_flow(
    Name="ddapi EMR Cluster",
    LogUri='s3://ddapi.data/logs',
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
            'Name': 'bootstrap test/install conda',
            'ScriptBootstrapAction': {
                'Path': 's3://ddapi.data/ddapp_emr_bootstrap.sh',
            }
        },
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    Configurations=[
        {
            "Classification": "spark-env",
            "Properties": {},
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    },
                    "Configurations": []
                }
            ]
        }
    ],

)

job_flow_id = response['JobFlowId']
print(job_flow_id)

response = emr_client.add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=[
                {
                    'Name': 'setup - copy files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 's3://ddapi.data/pyspark_setup.sh',
                                 '/home/hadoop/']
                    }
                },
                {
                    'Name': 'setup pyspark with conda',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'bash', '/home/hadoop/pyspark_setup.sh']
                    }
                }
            ]
        )

step_response = emr_client.add_job_flow_steps(
                 JobFlowId=job_flow_id,
                 Steps=[
                     {
                         'Name': 'setup - copy emr test py file',
                         'ActionOnFailure': 'CANCEL_AND_WAIT',
                         'HadoopJarStep': {
                             'Jar': 'command-runner.jar',
                             'Args': ['aws', 's3', 'cp',
                                      's3://ddapi.data/emr_test.py',
                                      '/home/hadoop/']
                     }
                     },
                     {
                        'Name': 'ddapp spark app test',
                        'ActionOnFailure': 'CANCEL_AND_WAIT',
                        'HadoopJarStep': {
                            'Jar': 's3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3',
                                     '--conf spark.yarn.executorEnv.PYSPARK_PYTHON=/usr/bin/python3'
                                    # '/home/hadoop/emr_test.py',
                                    's3://ddapi.data/emr_test.py']
                        }
                     }
                 ]
             )

steps_id = step_response['StepIds']
print("Step IDs:", steps_id)

response = emr_client.terminate_job_flows(
    JobFlowIds=[
        job_flow_id,
    ]
)



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
