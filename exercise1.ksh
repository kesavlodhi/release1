#!/usr/bin/ksh
###############################################################################
#                                                                             #
#       Script name - exerise1                                                #
#       Discription - This script is to check the send mail with details      #
#                     about the change file list in html report format        #
#                                                                             #
#       Date created:   06/04/2020                                            #
#                                                                             #
#       Author      :   Kesav Lodhi                                           #
#       Change History :                                                      #
###############################################################################
#setting variables
set -o xtrace
message="auto-commit from $USER@$(hostname -s) on $(date)"
GIT=`which git`
REPO_DIR=/mnt/d/Code_clone/release1
cd ${REPO_DIR}
LOCAL_REPOSITORY=new-feature
GITHUB_REPOSITORY=master

#To add, commit and push to code to the master repository
${GIT} checkout $GITHUB_REPOSITORY
${GIT} add --all .
${GIT} commit -m "$message"
#${GIT} push

