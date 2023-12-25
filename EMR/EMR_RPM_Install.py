import logging
from time import sleep
import os
import boto3
import getopt
import sys
"""
2023-09-20 Norris Created
介绍：
    该脚本用于对已建EMR，auto scaling时，新增的节点，执行rpm补丁包更新
    1. 脚本放置与EMR集群的Master节点
    2. 直接运行在后台，不通过 cron 调度。
    3. 
"""

logging.basicConfig(filename='log.txt',level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == '__main__':

    if (len(sys.argv) == 0):
        print("Usage: EMR_RPM_Install [-c clusterid]")
        sys.exit(0)

    ClusterID = ''
    opts,args = getopt.getopt(sys.argv[1:],"c:", \
                              ["clusterid="])
    for opt_name,opt_value in opts:
        if opt_name in ('-c','--clusterid'):
            ClusterID = opt_value
            logger.info("ClusterID:" + ClusterID)
        else:
            logger.info("need parameters [clusterid]")
            exit()

    FinishedList = []
    while True:
        #休眠5秒
        sleep(5)
        logger.info("Start")
        client = boto3.client('emr')
        ##获取 BOOTSTRAPPING 状态的节点，说明当前有新的节点正在启动
        response = client.list_instances(
            ClusterId=ClusterID,
            InstanceStates=['BOOTSTRAPPING'],
            InstanceGroupTypes=['CORE', 'TASK']
        )



        InstanceListBootstrapping = response['Instances']
        if len(InstanceListBootstrapping) > 0:
            # 记录当前启动的实例ID 在 BOOTSTRAPPING 状态时，就可以给实例打RPM补丁包
            for instanceID in InstanceListBootstrapping:

                Ec2InstanceId = instanceID['Ec2InstanceId']
                PrivateDnsName = instanceID['PrivateDnsName']
                if Ec2InstanceId in FinishedList:
                    logger.info("Instance is patched :" + Ec2InstanceId)
                    continue

                logger.info("Starting Instance:" + Ec2InstanceId)
                logger.info("Instance PrivateDnsName:" + PrivateDnsName)
                ##需要添加判断，是否已经打过补丁

                sshCommand = """aws s3 cp s3://my-s3-bucket-bj/replace-rpms.sh ./;
                chmod 755 replace-rpms.sh;
                ./replace-rpms.sh;
                """
                ret = os.system(f"ssh -i ~/cn-linux-key-pair.pem -o StrictHostKeyChecking=no {PrivateDnsName} \"{sshCommand}\"")
                logger.info(ret)
                logger.info("Remote Command Success - " + f"ssh -i ~/cn-linux-key-pair.pem -o StrictHostKeyChecking=no {PrivateDnsName} \"{sshCommand}\"")
                FinishedList.append(Ec2InstanceId)

