import boto3
from botocore.exceptions import WaiterError


class ClusterFun:
    """
    Class used to represent EMR cluster creation, job submission (spark),
    and spin down.
    """

    # Initializer / Instance Attributes
    def __init__(self,
                 region='us-east-1',
                 clustername='ClusterFun EMR Cluster',
                 logpath='s3://ddapi.data/logs'):
        self.region = region
        self.emr_client = boto3.client('emr', region_name=region)
        # create key for emr ec2 instance just in case need to SSH into cluster
        self.ec2 = boto3.client('ec2', region_name='us-east-1')
        self.key = 'emr_key'
        self.create_key_response = self.ec2.create_key_pair(KeyName=self.key)
        self.clustername = clustername
        self.logpath = logpath

    # write key-pair pem file method
    def storeKey(self, pem_path='emr_keypair.pem'):
        if not isinstance(pem_path, str):
            raise TypeError('pem_path must be string or None (default)')
        else:
            self.pem_path = pem_path
            emr_keypem = str(self.create_key_response['KeyMaterial'])
            with open(pem_path+'emr_keypair.pem', 'w') as f:
                f.write(emr_keypem)

    # cluster spin up method
    def spinUp(self):
        response = self.emr_client.run_job_flow(
            Name=self.clustername,
            LogUri=self.logpath,
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
                'Ec2KeyName': self.key,
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

        self.job_flow_id = response['JobFlowId']

        # get cluster id
        resp = self.emr_client.list_clusters()
        clus = resp['Clusters'][0]
        self.clusID = clus['Id']

        # don't forget to tip the waiter
        create_waiter = self.emr_client.get_waiter('cluster_running')
        try:
            create_waiter.wait(ClusterId=self.clusID,
                               WaiterConfig={
                                   'Delay': 15,
                                   'MaxAttempts': 120
                                   })

        except WaiterError as e:
            if 'Max attempts exceeded' in e.message:
                print('EMR Step did not complete in 30 minutes')
            else:
                print(e.message)

    def submaster(self):
        step_response = self.emr_client.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
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

        self.submaster_step_id = step_response['StepIds']
        print("Step IDs Running:", self.submaster_step_id)

        # don't forget to tip the waiter :)
        step_waiter = self.emr_client.get_waiter('step_complete')
        try:
            step_waiter.wait(ClusterId=self.clusID,
                             StepId=self.submaster_step_id[1],
                             WaiterConfig={
                                 'Delay': 15,
                                 'MaxAttempts': 240
                             })

        except WaiterError as e:
            if 'Max attempts exceeded' in e.message:
                print('EMR Step did not complete in 30 minutes')
            else:
                print(e.message)

    def spinDown(self):
        response = self.emr_client.terminate_job_flows(
            JobFlowIds=[self.job_flow_id]
            )
        # don't forget to tip the waiter :)
        spinDown_waiter = self.emr_client.get_waiter('cluster_terminated')
        try:
            spinDown_waiter.wait(ClusterId=self.clusID)

        except WaiterError as e:
            if 'Max attempts exceeded' in e.message:
                print('EMR Step did not complete in 30 minutes')
            else:
                print(e.message)

    # cluster key-pair delete method
    def deleteKey(self):
        self.key_del_response = self.ec2.delete_key_pair(KeyName=self.key)
