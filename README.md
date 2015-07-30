# Monitor_IntrospectionTool
Python script to automatically
#
1- launch a new cluster with two bootstrap actions:
#
    - install Spark
    #
    - upload the jar file "s3://zhonghao-emr-test/test/runnable.jar"
    #

2- launch a Spark step to run a Spark application
#

3- every 5 minutes, an HTTP get request is sent to the application to check if it is still running#
   a returned status code with value 405: means the application is running
   #
   a connection exception means it is down
   #

4- if the application is down, then the status of the cluster and the step are checked to see what caused the application to get down
and the cluster and/or the step are re-launched again#
#
Notes:
#
-the monitor_tool python file should be copied to the EC2 instance where the introspection interface is deployed
#