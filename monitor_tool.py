__author__ = 'ghadir'
import requests
import time
import boto3


class MonitorIntrospectionTool(object):
    SPARK_SUBMIT_STEP = \
        {
         'Jar':
            ('s3://us-east-1.elasticmapreduce/libs/'
             'script-runner/script-runner.jar'),
         'Args':
            ['/home/hadoop/spark/bin/spark-submit',
             '--deploy-mode',
             'cluster',
             '--conf',
             'k1=v1',
             's3://mybucket/myfolder/app.jar',
             'k2=v2'
             ]
        }

    def __init__(self):
        self.client = boto3.client('emr', region_name='us-east-1')
        print self.client
        self.job_flow_id = self.launch_cluster()
        self.job_flow_DNS = None
        self.step_id = self.launch_step()

    def server_status(self):
        monitor.job_flow_DNS = monitor.cluster_DNS()
        get_url = 'http://' + self.job_flow_DNS + ':3030/jobqueue'
        print get_url
        r = requests.get(get_url)
        output_string = str(r.status_code)
        print output_string
        return output_string

    def cluster_DNS(self):
        response = self.client.describe_cluster(ClusterId=self.job_flow_id)['Cluster']['MasterPublicDnsName']
        print response
        return response

    def cluster_status(self):
        response = self.client.describe_cluster(ClusterId=self.job_flow_id)['Cluster']['Status']['State']
        print response
        return response

    def step_status(self):
        response = self.client.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)['Step']['Status']['State']
        print response
        return response

    def launch_cluster(self):
        self.job_flow_id = self.client.run_job_flow(
            Name='IntrospectionSparkOnEMRGhadir',
            AmiVersion='3.8',
            Instances={
                'InstanceGroups': [
                    {
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'c3.xlarge',
                        'InstanceCount': 1
                    },
                    {
                        'InstanceRole': 'CORE',
                        'InstanceType': 'c3.2xlarge',
                        'InstanceCount': 10
                    }
                ],
                'Ec2KeyName': 'ghadir',
                'AdditionalMasterSecurityGroups': [
                    'sg-7556cf18',
                ]
            },
            Steps=[
                {
                    'Name': 'Start Spark History Server',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': [
                            's3://support.elasticmapreduce/spark/start-history-server'
                        ]
                    }
                },
            ],
            BootstrapActions=[
            {
                'Name': 'Install Spark',
                'ScriptBootstrapAction': {
                    'Path': 's3://support.elasticmapreduce/spark/install-spark'
                    }
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )['JobFlowId']
        print self.job_flow_id
        return self.job_flow_id

    def launch_step(self):
        self.step_id = self.client.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
            Steps=[
                {
                    'Name': 'SparkStep',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://zhonghao-emr-test/test/runnable.jar',
                        'MainClass': 'introspection.api.SparklyApp',
                        'Args': [
                            'client',
                        ]
                    }
                },
            ]
        )['StepIds'][0]
        print self.step_id
        return self.step_id

if __name__ == '__main__':
    monitor = MonitorIntrospectionTool()
    time.sleep(300)
    monitor.client.describe_cluster(ClusterId=monitor.job_flow_id)['Cluster']
    while True:
        try:
            monitor.server_status()
        except requests.exceptions.ConnectionError:
            print 'exception'
            step_status = monitor.step_status()
            cluster_status = monitor.cluster_status()
            if step_status == 'CANCELLED' or step_status == 'FAILED' or step_status == 'INTERRUPTED':
                if cluster_status == 'TERMINATING' or cluster_status == 'TERMINATED' or cluster_status == 'TERMINATED_WITH_ERRORS':
                    monitor.launch_cluster()
                    monitor.launch_step()
                else:
                    monitor.launch_step()

        time.sleep(300)



