from pyspark.sql import SparkSession, Row
import boto3
import json
# from aws_cred import ACCESS_KEY, SECRET_KEY

# connect to EMR client
emr_client = boto3.client('emr', region_name='us-east-1')

# create key for emr ec2 instance just in case need to SSH into cluster
# ec2 = boto3.client('ec2', region_name='us-east-1')
# create_key_response = ec2.create_key_pair(KeyName='ec2_emr_key')


# need to refactor/format emr job calls below
response = emr_client.run_job_flow(
    Name="ddapi EMR Cluster",
    LogUri='s3://ddapi.data/logs',
    ReleaseLabel='emr-5.23.0',
    Instances={
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm4.large',
                'InstanceCount': 2,
            }
        ],
        # 'Ec2KeyName': 'Dkan-key-supun',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        # 'Ec2SubnetId': 'string',
    },
    Applications=[
        {'Name': 'Hadoop'},
        {'Name': 'Spark'}
    ],
    BootstrapActions=[
        {
            'Name': 'bootstrap requirements',
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
print("Job", job_flow_id, " is running :)")

# no longer need below
# response = emr_client.add_job_flow_steps(
#    JobFlowId=job_flow_id,
#    Steps=[
#        {
#            'Name': 'setup - copy files',
#            'ActionOnFailure': 'CANCEL_AND_WAIT',
#            'HadoopJarStep': {
#                'Jar': 'command-runner.jar',
#                'Args': ['aws', 's3', 'cp',
#                         's3://ddapi.data/pyspark_setup.sh',
#                         '/home/hadoop/']
#            }
#        },
#        {
#            'Name': 'setup pyspark with conda',
#            'ActionOnFailure': 'CANCEL_AND_WAIT',
#            'HadoopJarStep': {
#                'Jar': 'command-runner.jar',
#                'Args': ['sudo', 'bash', '/home/hadoop/pyspark_setup.sh']
#            }
#        }
#    ]
# )
# no longer need above

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
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                         '--deploy-mode', 'cluster',
                         '--master', 'yarn',
                         's3://ddapi.data/emr_test.py']
            }
        }
    ]
)

steps_id = step_response['StepIds']
print("Step IDs Running:", steps_id)

response = emr_client.terminate_job_flows(
    JobFlowIds=[
        job_flow_id,
    ]
)

# delete key after job run
# key_del_response = client.delete_key_pair(
#    KeyName='my-key-pair',
# )
