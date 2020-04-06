import sys
import subprocess
import yaml
import logging
import datetime
import os
import paramiko
import boto3
import pandas as pd
import dateparser

sys.path.append(os.path.abspath('/home/ubuntu/vivek/samtrans-scripts/samtrans_etl'))
from utils import utilities as util
from utils.app_constants import statusConstants as sC
from utils.app_constants import levelConstants as lC
from utils.app_constants import errorConstants as eC

import sys

# reload(sys)
# sys.setdefaultencoding("utf-8")

sns = boto3.client('sns', region_name='us-west-2')
setting_path = "/home/ubuntu/vivek/samtrans-scripts/samtrans_etl/dno/python_scripts/config.yml"


def get_settings():
    with open(setting_path, 'r') as stream:
        try:
            data = (yaml.safe_load(stream))
            return data
        except Exception as e:
            logger.error('FAILED: LOADING CONFIG FILE' + str(e))


data = get_settings()
log_file_out = data["logs"]["LOGS_PATH"]
log_format = data["logs"]["format"]
log_datefmt = data["logs"]["datefmt"]
date_time = '{:%Y-%m-%d}.log'.format(datetime.datetime.now())
task = 'parse_dno'
dag = 'DNO Dag'
log_file_out = log_file_out + task + '_' + date_time
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=log_format, datefmt=log_datefmt, filename=log_file_out, filemode='a')

subject = data["SNS"]["subject"]
topicarn = data["SNS"]["topicarn"]

s3_bucket_prefix = data["paths"]["s3_bucket_prefix"]
incoming_xls_prefix = data["paths"]["incoming_xls_prefix"]
bucket = data["paths"]["bucket"]
processed_csv_path = data["paths"]["processed_csv_path"]
processing_csv_path = data["paths"]["processing_csv_path"]
processed_parq_path = data["paths"]["processed_parq_path"]
error_prefix = data["paths"]['error_prefix']
failed_files = []
cub_date_selection = data["paths"]['cub_date_selection']
print(cub_date_selection)


def get_dno_dist_files(all_files):
    dno_files = []
    for file in all_files:
        if "OpsControlLogData" in file:
            dno_files.append(file)
    return dno_files


def get_dno_cub_files(all_files):
    # print("here")
    dno_cub_files = []
    for file in all_files:
        if "CUB/DNO" in file:
            if "DNOs and DNCs 2017" in file and "updated" not in file:
                pass
            elif "2019" or "2018" or "updated" in file:
                dno_cub_files.append(file)
            else:
                pass
    # print("dno files da",dno_cub_files)
    return dno_cub_files


def write_to_s3(df, file):
    # print(file.split("/"))
    months = [g for n, g in df.groupby(pd.Grouper(key='RDate', freq='M'))]
    # print(len(months))
    if len(months) > 0:
        for df in months:
            date = pd.DatetimeIndex(df["RDate"][:1].values).date[0]
            # print(df.columns)
            month = str(date.month)
            year = str(date.year)
            filename = (file.split("/")[-1]).split(".")[-2]
            print("filename", filename)
            target_path_csv = processing_csv_path + 'DISTRICT/' + year + '/' + month + '/' + year + '_' + month + '_' + filename + ".csv"
            print("csv processing path", target_path_csv)
            # util.write_df_to_csv_on_s3(df,bucket,target_path)
            df.to_csv(s3_bucket_prefix + target_path_csv, index=False)
            # processing_path = target_path.replace('processed','incoming/processing')
            # util.write_df_to_csv_on_s3(df,bucket,processing_path)
            # df.to_csv(s3_bucket_prefix+processing_path,index=False)
            ##parq
            # target_parq_path = processing_csv_path.replace("csv","parquet")+'DISTRICT/'+year+'/'+month+'/'+year+'_'+month+'_'+filename+".parquet"
            # df.to_parquet(s3_bucket_prefix+target_parq_path, compression='snappy')

    else:
        failed_files.append({'file_name': file, 'reason': eC.INVALID_FILE, 'time': datetime.datetime.now()})
        raise Exception('Splitting data by monthwise return empty list ' + file)


def get_col_map(df2):
    col_map = {}
    for column in df2.columns[:10]:
        col_val_list = df2[column].tolist()
        if "DNO" in col_val_list:
            # print("YESSDNO",col_val_list)
            if "MILES" in col_val_list:
                col_map[column] = "dno_miles"
            else:
                col_map[column] = "dno"
        elif "DNC" in col_val_list:
            if "MILES" in col_val_list:
                col_map[column] = "dnc_miles"
            else:
                col_map[column] = "dnc"
        elif "DATE" in col_val_list:
            col_map[column] = "date"
        elif "LOCATION" in col_val_list and "ENDING" not in col_val_list:
            col_map[column] = "prob_location"
        elif "RUN#" in col_val_list:
            col_map[column] = "run"
        elif "Run#" in col_val_list:
            col_map[column] = "run"
        elif "Starting" in col_val_list:
            col_map[column] = "prob_location"
        elif "SCHEDULE" in col_val_list:
            col_map[column] = "schedule_number"
        elif "DNO " in col_val_list:
            if "MILES" in col_val_list:
                col_map[column] = "dno_miles"
    return col_map


def process_files_district(dno_files):
    for file in dno_files:
        try:
            target_cols = ["RDate", "Schedule", "Route", "fRun", "DNO", "DNOLost", "DNC", "DNCLost", "Type",
                           "Prob Location", "Prob Description", "Base", "Miles"]
            parse_dates = ["RDate"]
            df = pd.read_excel(s3_bucket_prefix + file, usecols=target_cols, parse_dates=parse_dates)
            df['Source'] = "District"
        except Exception as e:
            failed_files.append({'file_name': file, 'reason': eC.INVALID_FILE, 'time': datetime.datetime.now()})
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.RUNNING, level=lC.ERROR,
                                                     error=repr(e),
                                                     remarks='Column Validation failed for dno district ' + file,
                                                     reason=eC.INVALID_FILE, sns=True)
            logger.critical(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
            continue
        try:
            write_to_s3(df, file)
        except Exception as e:
            failed_files.append({'file_name': file, 'reason': eC.S3_WRITE_FAILED, 'time': datetime.datetime.now()})
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED, level=lC.ERROR,
                                                     error=repr(e), remarks='s3 Write:Failed ' + file,
                                                     reason=eC.S3_WRITE_FAILED, sns=True)
            logger.critical(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)


def get_df_by_sheet_mapping(data, month=None):
    sheets = []
    sheet_dict = {}
    for sheet in data.sheet_names:
        if 'Summary' not in str(sheet):
            if 'Sample' not in str(sheet):
                if month != None:
                    if month.lower() in sheet.lower():
                        sheets.append(sheet)
                        df = data.parse(sheet)
                        sheet_dict[sheet] = df
                        print(sheet, "yesss")
                else:
                    sheets.append(sheet)
                    df = data.parse(sheet)
                    sheet_dict[sheet] = df
            else:
                pass
    # print("keys are here sheet",sheet_dict.keys())
    return sheet_dict


def write_dno_cub_to_s3(new_mapping, file):
    filename = (file.split("/")[-1]).split(".")[-2]
    print("heyy:", filename)
    for date in new_mapping:
        try:
            month = datetime.datetime.strftime(date, '%b')
            year = date.year
            target_path_csv = processing_csv_path + 'CUB' + '/' + str(year) + '/' + str(month) + '/' + 'CUB_' + str(
                year) + '_' + str(month) + '_' + filename + '.csv'
            print("cub path", target_path_csv)
            # processing_path = processed_path.replace('processed','incoming/processing')
            # print(processing_path)
            df = new_mapping[date]
            # util.write_df_to_csv_on_s3(new_mapping[date],bucket,processed_path)
            df.to_csv(s3_bucket_prefix + target_path_csv, index=False)
            # util.write_df_to_csv_on_s3(new_mapping[date],bucket,processing_path)
            # df.to_csv(s3_bucket_prefix+processed_path,index=False)
        except Exception as e:
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.RUNNING, level=lC.ERROR,
                                                     error=repr(e),
                                                     remarks='CUB CSV Write from sheet:Failed for ' + date,
                                                     reason=eC.CSV_PROCESSING_FAILED, sns=True)
            logger.critical(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)


def process_dno_cub(cub_files, month=None):
    res_list = []
    for file in cub_files:
        # print(file)
        try:
            data = pd.ExcelFile(s3_bucket_prefix + file)
            sheet_df_mapping = get_df_by_sheet_mapping(data, month)
            new_mapping = parse_sheets(sheet_df_mapping)
            # print("keys",new_mapping.keys())
            res_list.append(new_mapping)
            if len(new_mapping.keys()) < 1:
                # print("No sheets")
                pass
            else:
                write_dno_cub_to_s3(new_mapping, file)
        except Exception as e:
            failed_files.append(
                {'file_name': file, 'reason': eC.CSV_PROCESSING_FAILED, 'time': datetime.datetime.now()})
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.RUNNING, level=lC.ERROR,
                                                     error=repr(e), remarks=' CSV_PROCESSING_FAILED:Failed ' + file,
                                                     reason=eC.CSV_PROCESSING_FAILED, sns=True)
            logger.critical(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)


def parse_sheets(sheet_dict):
    cols_of_interest = ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5",
                        "Unnamed: 6", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9"]
    new_dic = {}
    dno_dnc_cols = ["date", "run", "schedule_number", "dno_miles", "dnc_miles", "dno", "dnc", "prob_location"]
    for key in sheet_dict.keys():
        try:
            if sheet_dict[key].empty or key == "Sept 2017":
                log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.RUNNING, level=lC.WARNING,
                                                         remarks='Empty Sheet for ' + key,
                                                         reason=eC.CSV_PROCESSING_FAILED, sns=True)
                logger.warning(log_msg)
                sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
            else:
                # print(key)
                # print(sheet_dict[key].columns)
                df = sheet_dict[key]
                col_map = get_col_map(df)
                new_df = df.rename(columns=col_map)
                # print("new cols",new_df.columns)
                # print("dno cols",dno_dnc_cols)
                if "prob_location" not in col_map.keys():
                    dno_dnc_col = ["date", "run", "schedule_number", "dno_miles", "dnc_miles", "dno", "dnc"]
                    new_df = new_df[dno_dnc_col]
                else:
                    new_df = new_df[dno_dnc_cols]
                final_df = new_df[new_df['date'].notna()]
                final_df = final_df[final_df['date'].apply(
                    lambda x: isinstance(dateparser.parse(str(x), settings={'STRICT_PARSING': True}),
                                         datetime.datetime))]
                final_df["source"] = "CUB"
                final_df['route'] = final_df.apply(
                    lambda row: str(row.schedule_number)[:3] if len(str(row.schedule_number)) > 3 else " ", axis=1)
                final_df['miles'] = final_df.apply(
                    lambda row: row.dnc_miles if row.dnc == 1 else (row.dno_miles if row.dno == 1 else " "), axis=1)
                final_df['dno'] = final_df.apply(lambda row: True if row.dno == 1 else False, axis=1)
                final_df['dnc'] = final_df.apply(lambda row: True if row.dnc == 1 else False, axis=1)
                # print(final_df[["dno",'date']])
                date = final_df['date'].iloc[0]
                new_dic[date] = final_df
        except Exception as e:
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED, level=lC.ERROR,
                                                     error=repr(e), remarks='Sheet parsing failed for CUB: ' + key,
                                                     reason=eC.INVALID_FILE, sns=True)
            logger.critical(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
    return new_dic


def move_failed_files(failed_files):
    for file in failed_files:
        target_path = error_prefix + file['time'].date().strftime("%m-%d-%Y") + '/' + file['reason'] + '/' + \
                      file['file_name'].split('/')[-1]
        util.copy_file(file['file_name'], bucket, target_path)


def main():
    all_files = []
    try:
        # print(incoming_xls_prefix)
        all_files = util.get_s3_keys_by_prefix(bucket, incoming_xls_prefix)
        # print(all_files)
    except Exception as e:
        log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED, level=lC.ERROR, error=repr(e),
                                                 remarks='In S3 Key fetch -  all_files', reason=eC.S3_CONNECTION_ERROR,
                                                 sns=True)
        logger.error(log_msg)
        sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
        sys.exit(1)

    if len(all_files) > 1:
        try:
            dno_dist_files = get_dno_dist_files(all_files)
            dno_cub_files = get_dno_cub_files(all_files)
        except Exception as e:
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED, level=lC.WARNING,
                                                     remarks='ERROR IN getting cub/district file', exit_code=1,
                                                     reason=eC.ERROR_IN_PARSING_FILENAME, sns=True)
            logger.error(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
            sys.exit(1)

        try:
            # print(dno_cub_files)
            useful_files = dno_cub_files + dno_dist_files
            # print("useful",useful_files)
            bad_files = set(all_files) - set(useful_files)
            util.delete_file_list(bucket, list(bad_files))
        except Exception as e:
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FAILED, level=lC.WARNING,
                                                     remarks='ERROR in removing unwanted files',
                                                     reason=eC.S3_DELETE_FAILED, sns=True)
            logger.error(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
            sys.exit(1)

        if len(dno_dist_files) < 1:
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.RUNNING, level=lC.CRITICAL,
                                                     error=repr(e),
                                                     remarks='DNO District Files to be processed missing in incoming',
                                                     reason=eC.NO_FILES_FOUND, sns=True)
            logger.error(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)

        else:
            try:
                process_files_district(dno_dist_files)
                pass
            except Exception as e:
                log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.RUNNING, level=lC.CRITICAL,
                                                         error=repr(e), remarks='ERROR in process_files_district',
                                                         reason=eC.CSV_PROCESSING_FAILED, sns=True)
                logger.error(log_msg)
                sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
        if len(dno_cub_files) < 1:
            log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.RUNNING, level=lC.CRITICAL,
                                                     error=repr(e),
                                                     remarks='DNO CUB Files to be processed missing in incoming',
                                                     reason=eC.NO_FILES_FOUND, sns=True)
            logger.error(log_msg)
            sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)
        else:
            try:
                if len(cub_date_selection) > 0:
                    year = cub_date_selection[0]['year']
                    month = cub_date_selection[0]['month']
                    for file in dno_cub_files:
                        if str(year) in file:
                            process_dno_cub([file], month)
                else:
                    print("processing dno_cub_files")
                    process_dno_cub(dno_cub_files)
            except Exception as e:
                log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.RUNNING, level=lC.CRITICAL,
                                                         error=repr(e), remarks='ERROR in process_dno_cub',
                                                         reason=eC.CSV_PROCESSING_FAILED, sns=True)
                logger.error(log_msg)
                sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)

        if len(failed_files) > 0:
            print("moving_failed_files")
            move_failed_files(failed_files)
        else:
            pass


    else:
        log_msg, sns_msg = util.get_log_template(dag=dag, task=task, status=sC.FINISHED, level=lC.WARNING,
                                                 remarks='Files to be processed missing in incoming',
                                                 reason=eC.NO_FILES_FOUND, sns=True)
        logger.error(log_msg)
        sns.publish(TopicArn=topicarn, Subject=subject, Message=sns_msg)


if __name__ == "__main__":
    main()

