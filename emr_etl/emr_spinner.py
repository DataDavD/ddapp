import boto3
from botocore.exceptions import WaiterError

# connect to EMR client
emr_client = boto3.client('emr', region_name='us-east-1')

# create key for emr ec2 instance just in case need to SSH into cluster
ec2 = boto3.client('ec2', region_name='us-east-1')
create_key_response = ec2.create_key_pair(KeyName='ec2_emr_key')
unkey = str(create_key_response['KeyMaterial'])
with open('testkey.pem', 'w') as f:
    f.write(unkey)

response = emr_client.run_job_flow(
    Name="ddapi EMR Cluster4",
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
        'Ec2KeyName': 'ec2_emr_key',
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
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3",
                        "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                        }
                }
            ]
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.sql.execution.arrow.enabled": "true"
                }
        },
        {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
                }
        }
    ],
)

job_flow_id = response['JobFlowId']
print("Job", job_flow_id, "is running")

# get cluster id
resp = emr_client.list_clusters()
clus = resp['Clusters'][0]
clusID = clus['Id']

create_waiter = emr_client.get_waiter('cluster_running')
try:
    create_waiter.wait(ClusterId=clusID,
                       WaiterConfig={
                           'Delay': 15,
                           'MaxAttempts': 120
                       })

except WaiterError as e:
    if 'Max attempts exceeded' in e.message:
        print('EMR Step did not complete in 30 minutes')
    else:
        print(e.message)

# don't forget to tip the waiter :)


step_response = emr_client.add_job_flow_steps(
    JobFlowId=job_flow_id,
    Steps=[
        {
            'Name': 'setup - copy emr test py file',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['aws', 's3', 'cp',
                         's3://ddapi.data/ddpyspark_etl_script.py',
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
                         's3://ddapi.data/ddpyspark_etl_script.py']
            }
        }
    ]
)

steps_id = step_response['StepIds']
print("Step IDs Running:", steps_id)

step_waiter = emr_client.get_waiter('step_complete')
try:
    step_waiter.wait(ClusterId=clusID,
                     StepId=steps_id[1],
                     WaiterConfig={
                         'Delay': 15,
                         'MaxAttempts': 240
                     })

except WaiterError as e:
    if 'Max attempts exceeded' in e.message:
        print('EMR Step did not complete in 30 minutes')
    else:
        print(e.message)

# don't forget to tip the waiter :)

response = emr_client.terminate_job_flows(
    JobFlowIds=[job_flow_id]
    )

spinDown_waiter = emr_client.get_waiter('cluster_terminated')
try:
    spinDown_waiter.wait(ClusterId=clusID)

except WaiterError as e:
    if 'Max attempts exceeded' in e.message:
        print('EMR Step did not complete in 30 minutes')
    else:
        print(e.message)

# don't forget to tip the waiter :)

# delete key after job run
key_del_response = ec2.delete_key_pair(KeyName='ec2_emr_key')
