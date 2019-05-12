import boto3
from botocore.exceptions import WaiterError
from aws_cred import ACCESS_KEY, SECRET_KEY


class ClusterFun:
    """
    Class used to represent EMR cluster creation, job submission (spark),
    and spin down.

    Attributes
    ----------
    region : str
        specify AWS region
    clustername : str
        name of EMR cluster
    logpath : str
        S3 path to store EMR Cluster logs

    Methods
    -------
    storeKey(pem_path='emr_keypair.pem')
        saves ec2 key-pair pem file to specified Path
    spinUp(self,
               btstrap_loc='s3://ddapi.data/ddapp_emr_bootstrap.sh',
               mstr_cnt=1,
               mstr_mkt='ON_DEMAND',
               slave_cnt=2,
               slave_mkt='ON_DEMAND',
               )
        spins up EMR Cluster to user specifications
    __step_waiter(step_id)
        private method for waiting on EMR job flow steps to complete
    spksub_step(self,
                s3_file_path='s3://ddapi.data/ddpyspark_etl_script.py')
        submits spark job as EMR job flow step
    spinDown()
        spins down EMR Cluster
    deleteKey()
        deletes EC2 Master node key-pair
    """

    # Initializer / Instance Attributes
    def __init__(self,
                 region='us-east-1',
                 ACCESS_KEY=ACCESS_KEY,
                 SECRET_KEY=SECRET_KEY,
                 clustername='ClusterFun EMR Cluster',
                 logpath='s3://ddapi.data/logs'):
        self.region = region
        self.emr_client = boto3.client('emr',
                                       aws_access_key_id=ACCESS_KEY,
                                       aws_secret_access_key=SECRET_KEY,
                                       region_name=region)
        # create key for emr ec2 instance just in case need to SSH into cluster
        self.ec2 = boto3.client('ec2',
                                aws_access_key_id=ACCESS_KEY,
                                aws_secret_access_key=SECRET_KEY,
                                region_name='us-east-1')
        self.key = 'emr_key'
        self.create_key_response = self.ec2.create_key_pair(KeyName=self.key)
        self.clustername = clustername
        self.logpath = logpath

    def __repr__(self):
        return (f'{self.__class__.__name__}('
                f'{self.clustername}, {self.region}, {self.logpath})')

    def __str__(self):
        return f'an EMR Cluster named {self.clustername} in the {self.region}'

    # write key-pair pem file method
    def storeKey(self, pem_path='emr_keypair.pem'):
        if not isinstance(pem_path, str):
            raise TypeError('pem_path must be string')
        else:
            self.pem_path = pem_path
            emr_keypem = str(self.create_key_response['KeyMaterial'])
            with open(pem_path, 'w') as f:
                f.write(emr_keypem)

    # cluster spin up method
    def spinUp(self,
               btstrap_loc='s3://ddapi.data/ddapp_emr_bootstrap.sh',
               mstr_cnt=1,
               mstr_mkt='ON_DEMAND',
               slave_cnt=2,
               slave_mkt='ON_DEMAND',
               ):
        self.btstrap_loc = btstrap_loc
        response = self.emr_client.run_job_flow(
            Name=self.clustername,
            LogUri=self.logpath,
            ReleaseLabel='emr-5.23.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': mstr_mkt,
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm4.large',
                        'InstanceCount': mstr_cnt,
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': slave_mkt,
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm4.large',
                        'InstanceCount': slave_cnt,
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
                        'Path': btstrap_loc,
                        }
                },
                ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Configurations=[
                {
                    'Classification': 'spark-env',
                    'Configurations': [
                        {
                            'Classification': 'export',
                            'Properties': {
                                'PYSPARK_PYTHON': '/usr/bin/python3',
                                'PYSPARK_DRIVER_PYTHON': '/usr/bin/python3'
                                }
                        }
                        ]
                },
                {
                    'Classification': 'spark-defaults',
                    'Properties': {
                        'spark.sql.execution.arrow.enabled': 'true'
                        }
                },
                {
                    'Classification': 'spark',
                    'Properties': {
                        'maximizeResourceAllocation': 'true'
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
                                   'MaxAttempts': 480
                                   })

        except WaiterError as e:
            if 'Max attempts exceeded' in e.message:
                print('EMR Cluster did not finish spinning up in two hours')
            else:
                print(e.message)

    def __step_waiter(self, step_id):
        """
        step waiter for spksub_set func
        """
        # don't forget to tip the waiter :)
        step_waiter = self.emr_client.get_waiter('step_complete')
        try:
            step_waiter.wait(ClusterId=self.clusID,
                             StepId=step_id[0],
                             WaiterConfig={
                                 'Delay': 15,
                                 'MaxAttempts': 480
                             })

        except WaiterError as e:
            if 'Max attempts exceeded' in e.message:
                print('EMR Step did not complete in two hours')
            else:
                print(e.message)

    def spksub_step(self,
                    s3_file_path='s3://ddapi.data/ddpyspark_etl_script.py'):
        step_response = self.emr_client.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
            Steps=[
                {
                    'Name': 'ddapp spark app test',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--deploy-mode', 'cluster',
                                 '--master', 'yarn',
                                 s3_file_path]
                        }
                }
            ]
            )

        spksub_step_id = step_response['StepIds']
        print("Step IDs Running:", spksub_step_id)

        self.__step_waiter(step_id=spksub_step_id)

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
