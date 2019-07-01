import logging
import boto3
from botocore.exceptions import WaiterError
from aws_cred import ACCESS_KEY, SECRET_KEY

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class ClusterFun:
    """
    Class used to represent EMR cluster creation, job submission (spark),
    and spin down.

    Methods
    -------
    storeKey(pem_path='emr_keypair.pem')
        saves ec2 key-pair pem file to specified Path
    spinUp(self,
           btstrap_loc='s3://ddapi.data/ddapp_emr_bootstrap.sh',
           mstr_cnt=1,
           mstr_mkt='ON_DEMAND',
           slave_cnt=2,
           slave_mkt='ON_DEMAND')
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
        """
        Parameters
        ----------
        region : str, optional
            AWS region (default is 'us-east-1')
        ACCESS_KEY: str
            user's AWS access key ID
        SECRET_KEY: str
            user's AWS secrete access key ID
        clustername : str, optional
            name of EMR cluster (default is 'ClusterFun EMR Cluster')
        logpath : str, optional
            S3 path to store EMR Cluster logs
            (default is 's3://ddapi.data/logs')
        """

        self.region = region
        self.emr_client = boto3.client('emr',
                                       aws_access_key_id=ACCESS_KEY,
                                       aws_secret_access_key=SECRET_KEY,
                                       region_name=region)
        logging.info(f'connecting to emr_client: {self.emr_client}')
        # create key for emr ec2 instance just in case need to SSH into cluster
        self.ec2 = boto3.client('ec2',
                                aws_access_key_id=ACCESS_KEY,
                                aws_secret_access_key=SECRET_KEY,
                                region_name='us-east-1')
        logging.info(f'connecting to ec2_client: {self.ec2}')
        self.key = 'emr_key'
        self.create_key_response = self.ec2.create_key_pair(KeyName=self.key)
        logging.info('emr ec2 key created')
        self.clustername = clustername
        self.logpath = logpath
        logging.info(f'logging path set to {self.logpath}')

    def __repr__(self):
        return (f'{self.__class__.__name__}('
                f'{self.clustername}, {self.region}, {self.logpath})')

    def __str__(self):
        return f'an EMR Cluster named {self.clustername} in the {self.region}'

    # write key-pair pem file method
    def storeKey(self, pem_path='emr_keypair.pem'):
        """
        Saves the aws key-pair pem file to user specified path & path. Default
        path is current working directory.  Default file name is
        emr_keypair.pem

        Parameters
        ----------
        pem_path : str, optional
            Path and file name to save key-pair pem (default is
            'emr_keypair.pem')

        Raises
        ------
        TypeError
            If pem_path parameter is not a string
        """
        try:
            isinstance(pem_path, str)
            self.pem_path = pem_path
            emr_keypem = str(self.create_key_response['KeyMaterial'])
            with open(pem_path, 'w') as f:
                f.write(emr_keypem)
        except TypeError as error:
            print(f'Error occurred: {error}; pem_path must be str')
            logging.error(f'Error occurred: {error}; pem_path must be str', exc_info=True)

    # cluster spin up method
    def spinUp(self,
               btstrap_loc='s3://ddapi.data/ddapp_emr_bootstrap.sh',
               mstr_cnt=1,
               mstr_mkt='ON_DEMAND',
               slave_cnt=2,
               slave_mkt='ON_DEMAND',
               ):
        """
        Spins up EMR Cluster

        Parameters
        ----------
        btstrap_loc : str
            S3 location of bootstrap bash script
            (default is 's3://ddapi.data/ddapp_emr_bootstrap.sh')
        mstr_cnt: int, optional
            master node count (default is 1)
        mstr_mkt: str, optional
            master market type (default is 'ON_DEMAND')
        slave_cnt: int, optional
            slave node count (default is 1)
        slave_mkt: str, optional
            slave market type (default is 'ON_DEMAND')
        """

        self.btstrap_loc = btstrap_loc
        logging.info(f'starting to spin up emr cluster from emr client: {self.emr_client}')
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
        logging.info(f'spinning up emr cluster from emr client: {self.emr_client}')
        self.job_flow_id = response['JobFlowId']
        logging.info(f'job flow id {self.emr_client} logged')

        # get cluster id
        resp = self.emr_client.list_clusters()
        clus = resp['Clusters'][0]
        self.clusID = clus['Id']

        # don't forget to tip the waiter
        logging.info(f'start waiter')
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
        Private class method for waiting on EMR job flow steps

        Parameters
        ----------
        step_id : str
            jobflow step_id of current running step
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
        """
        Runs jobflow step on running EMR cluster

        Parameters
        ----------
        s3_file_path : str
            S3 location of PySpark script
            (default is 's3://ddapi.data/ddapp_emr_bootstrap.sh'))
        """

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

        self.__step_waiter(step_id=spksub_step_id)

    def spinDown(self):
        """
        Spins down running EMR cluster
        """

        self.emr_client.terminate_job_flows(JobFlowIds=[self.job_flow_id])
        # don't forget to tip the waiter :)
        spinDown_waiter = self.emr_client.get_waiter('cluster_terminated')
        try:
            spinDown_waiter.wait(ClusterId=self.clusID)

        except WaiterError as e:
            if 'Max attempts exceeded' in e.message:
                print('EMR Step did not complete in 30 minutes')
            else:
                print(e.message)

    def deleteKey(self):
        """
        Deletes key-pair created in EMR cluster created in __init__
        """

        self.key_del_response = self.ec2.delete_key_pair(KeyName=self.key)
