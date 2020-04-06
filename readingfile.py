import sys
import subprocess
import yaml
import logging
import datetime
import os
import paramiko
import boto3

logger = logging.getLogger(__name__)
date_time = '{:%Y-%m-%d}.log'.format(datetime.datetime.now())

sys.path.append(os.path.abspath('/home/ubuntu/projects/samtrans-scripts/samtrans_etl'))
from utils import utilities as util
from utils.app_constants import statusConstants as sC
from utils.app_constants import levelConstants as lC
from utils.app_constants import errorConstants as eC

sns = boto3.client('sns', region_name='us-west-2')
setting_path = "/home/ubuntu/rajat/samtrans-scripts/samtrans_etl/clipper/python_scripts/settings.yml"

def get_settings():
    with open(setting_path, 'r') as stream:
        try:
            data = (yaml.safe_load(stream))
            return data
        except Exception as e:
            logger.error('FAILED: LOADING CONFIG FILE' + str(e))


def check_file(file):
    try:
        datetime_object = datetime.datetime.strptime(file, '%B %Y')
        return 1
    except Exception as e:
        logger.warning('Invalid File name' + str(e))
        return 0

data = get_settings()
log_file_out = data["logs"]["logs_path"]
log_format = data["logs"]["format"]
log_datefmt = data["logs"]["datefmt"]
task = 'move_sftp_to_s3'
dag = 'Clipper Dag'
log_file_out = log_file_out + task + '_' + date_time
logging.basicConfig(level=logging.INFO, format=log_format, datefmt=log_datefmt, filename=log_file_out, filemode='a')

destination_sf_ud = data["paths"]['clipper_s3_inc_sf_ud']
destination_sys_cd = data["paths"]['clipper_s3_inc_sys_cd']

subject = data["SNS"]["subject"]
topicarn = data["SNS"]["topicarn"]

def check_input_ready(data):
    inputs = {}
    try:
        inputs["source_sf_ud"] = data["paths"]["clipper_sftp_sf_ud"]
        inputs["source_sys_cd"] = data["paths"]["clipper_sftp_sys_cd"]
    except Exception as e:
        log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED,
                                                 level=lC.CRITICAL, error=repr(e),
                                                 remarks=None, reason=eC.CONFIG_ERROR, exit_code=1, sns=True)
        logger.error(log_msg)
        sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
        sys.exit(1)

    try:
        inputs["host"] = data['sftp']['host']
        inputs["user"] = data['sftp']['user']
        inputs["sshk"] = data['sftp']['sshk']
        return inputs
    except Exception as e:
        log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED,
                                                 level=lC.CRITICAL, error=repr(e),
                                                 remarks=None, reason=eC.NO_SFTP_CREDENTIALS, exit_code=1, sns=True)
        logger.error(log_msg)
        sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
        sys.exit(1)


def parse_bash_out(output):
    #print(output)
    files = str(output).split("\n")
    #print(files)
    passed = []
    for file in files:
        #print(file)
        flag = True #check_file((file.split(".")[0]))
        if flag:
            passed.append(file)
    return passed


def get_sftp_connection(host, user, SSHK):
    try:
        sshcon = paramiko.SSHClient()  # will create the object
        sshcon.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # no known_hosts error
        sshcon.connect(host, username=user, key_filename=SSHK)  # no passwd needed
        logger.info('SUCCEEDED:CONNECTION to SFTP')
        return sshcon
    except Exception as e:
        log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED,
                                                 level=lC.CRITICAL, error=repr(e),
                                                 remarks=None, reason=eC.SFTP_CONN_ERROR, exit_code=1, sns=True)
        logger.error(log_msg)
        sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
        sys.exit(1)


def main():
    inputs = check_input_ready(data)
    copy_cmd_ud = ('aws s3 cp %s %s --recursive --exclude "newfiles"') % (inputs["source_sf_ud"], destination_sf_ud)
    copy_cmd_cd = ('aws s3 cp %s %s --recursive --exclude "newfiles"') % (inputs["source_sys_cd"], destination_sys_cd)
    list_file_cmd_ud = ('ls {}').format(inputs["source_sf_ud"])
    list_file_cmd_cd = ('ls {}').format(inputs["source_sys_cd"])
    sshcon = get_sftp_connection(inputs["host"], inputs["user"], inputs["sshk"])
    try:
        stdin, stdout, stderr = sshcon.exec_command(list_file_cmd_ud)
        stdin, stdout, stderr = sshcon.exec_command(list_file_cmd_cd)
        output = stdout.read().decode('ascii')
        input_files = parse_bash_out(output)
        msg = util.get_log_template(dag=dag, task=task, status=sC.RUNNING,
                                    level=lC.INFO,
                                    remarks='Bash List cmd Executed')
        logger.info(msg)
    except Exception as e:
        log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED,
                                                 level=lC.CRITICAL, error=repr(e),
                                                 remarks='SFTP file listing failed', reason=eC.BASH_ERROR, exit_code=1,
                                                 sns=True)
        logger.error(log_msg)
        sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
        sys.exit(1)

    if len(input_files) < 1:
        log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FINISHED,
                                                 level=lC.WARNING, reason=eC.NO_FILES_FOUND,
                                                 remarks='No Files to Process', exit_code=1, sns=True)
        logger.warning(log_msg)
        sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
        sys.exit(1)

    else:
        try:
            logger.info(msg)
            stdin, stdout, stderr = sshcon.exec_command(copy_cmd_ud)
            stdin, stdout, stderr = sshcon.exec_command(copy_cmd_cd)
            output = stdout.read()
            stderr_out = stderr.read()
            if len(stderr_out) == 0:
                log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FINISHED,
                                                         level=lC.INFO, remarks='SUCCESS-COPY SFTP to S3', sns=True)
                sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
            else:
                log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED,
                                                         level=lC.CRITICAL, error=repr(stderr_out),
                                                         remarks='Copy Command sftp to s3 failed',
                                                         reason=eC.S3_COPY_FAILED, exit_code=1, sns=True)
                logger.error(log_msg)
                sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
                sys.exit(1)
        except Exception as e:
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED,
                                                     level=lC.CRITICAL, error=repr(e),
                                                     remarks='Copy Command sftp to s3 failed', reason=eC.S3_COPY_FAILED,
                                                     exit_code=1, sns=True)
            logger.error(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
            sys.exit(1)
    log_msg = util.get_log_template(dag=dag, task=task, status=sC.FINISHED, level=lC.INFO)
    logger.info(log_msg)


if __name__ == "__main__":
    main()
