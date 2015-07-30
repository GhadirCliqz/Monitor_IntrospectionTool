__author__ = 'ghadir'
import requests
import time
import boto3

class MonitorIntrospectionTool(object):

    def __init__(self):
        self.client = boto3.client('emr', region_name='us-east-1')
        print self.client
        self.job_flow_id = self.launch_cluster()
        #time.sleep(240)
        #self.add_Jarfile()
        self.step_id = self.launch_step()

    # send an http get request to the master cluster node to check if it is running
    # status code 405: it is running
    # exception: it is down
    def server_status(self):
        self.job_flow_DNS = self.cluster_DNS()
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

    # launch a cluster with a step and bootstrap action to install Spark
    def launch_cluster(self):
        self.job_flow_id = self.client.run_job_flow(
            Name='IntrospectionSparkOnEMRGhadir',
            AmiVersion='3.8',
            Instances={
                'InstanceGroups': [
                    {
                        'Market': 'SPOT',
                        'InstanceRole': 'MASTER',
                        'BidPrice': '0.2',
                        'InstanceType': 'c3.xlarge',
                        'InstanceCount': 1
                    },
                    {
                        'Market': 'SPOT',
                        'InstanceRole': 'CORE',
                        'BidPrice': '0.2',
                        'InstanceType': 'd2.2xlarge',
                        'InstanceCount': 20
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
            },
            {
                'Name': 'Add Jar File',
                'ScriptBootstrapAction': {
                    'Path': 's3://ghadir.introspection-tool/add_Jarfile'
                    }
            }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Tags=[
                {
                    'Key': 'Owner',
                    'Value': 'ghadir@cliqz.com'
                },
                {
                    'Key': 'Project',
                    'Value': 'Introspection'
                }
            ]
        )['JobFlowId']
        print self.job_flow_id
        return self.job_flow_id

    # launch a Spark step
    def launch_step(self):
        self.step_id = self.client.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
            Steps=[
                {
                    'Name': 'Spark Application',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': [
                                '/home/hadoop/spark/bin/spark-submit', '--class', 'introspection.api.SparklyApp',
                                '--deploy-mode','client', '/home/hadoop/runnable2.jar'
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
                    #time.sleep(240)
                    #monitor.add_Jarfile()
                    monitor.launch_step()
                else:
                    monitor.launch_step()

        time.sleep(300)



