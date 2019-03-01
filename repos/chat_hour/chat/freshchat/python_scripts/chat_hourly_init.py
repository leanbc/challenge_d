import config
import sys
import linecache
import os
import fnmatch
import logging
import psycopg2
from datetime import date, timedelta
import datetime
from flask import Flask
import boto3
from botocore.exceptions import ClientError
import subprocess

# example run command: python3 ~/repositories/chat*/freshchat/python_scripts/chat_init.py 'dev' '2019-01-10'

app = Flask(__name__)

# by default process yesterday's files
file_date = str(sys.argv[2]) if len(sys.argv) > 2 else datetime.datetime.now().strftime("%Y-%m-%d_%H-00-00")

file_date ='2019-02-01_07-00-00'


# We have 2 kind of files:
#       1-Raw report.
#       2-AgentPerformance, with grouped data by Agent.
# Both of this files have 2 versions (UE and US) denoted by the number 1 or 3 after de word "Klarna"
# in the name of the file


file_name_pattern = 'Freshchat-Hourly-Reports-Raw-Klarna*-'+ file_date +'.csv'
file_name_pattern_agents= 'Freshchat-Reports-AgentPerformance-Klarna*-'+file_date+'.csv'
bucket_name = 'eu-production-klarna-data-freshchat-transfer'



class FreshchatDaily:
    def __init__(self):


        try:

            env = sys.argv[1] if len(sys.argv) > 1 else 'dev'

            if env == 'dev':
                app.config = config.DevConfigHourly
            elif env == 'live':
                app.config = config.LiveConfigHourly
            else:
                raise ValueError('Invalid environment name')

            logging.basicConfig(
                                level = logging.DEBUG,
                                format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                datefmt = '%m-%d %H:%M:%S',
                                # filename = app.config.LOG_PATH + 'freshchat-hourly-'+ str(datetime.datetime.now().strftime("%Y%m%d-%H%M%S")) +'.log',
                                # filename = app.config.LOlen(sys.argv)G_PATH + 'freshchat-daily-'+ str(datetime.datetime.now().strftime("%Y%m%d")) +'.log',
                                # filemode = 'w'
            )

            logging.info('Initialised : ' + sys.argv[0] + ', For environment: ' + env)

        except:
            print("***************__init__: something went wrong, see the traceback below....***************")
            raise

    def print_exception(self):

        try:

            exc_type, exc_obj, tb = sys.exc_info()
            f = tb.tb_frame
            lineno = tb.tb_lineno
            filename = f.f_code.co_filename
            linecache.checkcache(filename)
            line = linecache.getline(filename, lineno, f.f_globals)
            logging.info('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))

        except:
            logging.exception("***************print_exception: something went wrong, see the traceback below....***************")
            raise

    def make_db_connection(self):
        try:
            logging.info('make_db_connection: started')

            self.connection = psycopg2.connect(
                                                host=app.config.DB_HOST,
                                                user=app.config.DB_USER,
                                                password=app.config.DB_PASSWORD,
                                                dbname=app.config.DB_NAME,
                                                sslmode=app.config.DB_SSLMODE
            )

            self.connection.autocommit = True
            self.cursor = self.connection.cursor()


            logging.info('DB connection successful to host: ' + app.config.DB_HOST + ', dbname: ' + app.config.DB_NAME)
        except:
            logging.exception("***************make_db_connection: something went wrong, see the traceback below....***************")

    # def download_source_data_files(self):
    #
    #     try:
    #         s3_source_path = app.config.S3_SOURCE_PATH
    #         s3_destination_path = app.config.S3_DESTINATION_PATH
    #         local_source_file_path = app.config.LOCAL_SOURCE_FILE_PATH
    #
    #         file_array = ['Freshchat-Hourly-Reports-Raw-Klarna1-' + file_date + '.csv',
    #                       'Freshchat-Hourly-Reports-Raw-Klarna3-' + file_date + '.csv',
    #                       'Freshchat-Reports-AgentPerformance-Klarna1-' + file_date + '.csv',
    #                       'Freshchat-Reports-AgentPerformance-Klarna3-' + file_date + '.csv'
    #                       ]

            # for file_name in file_array:


                # boto3.setup_default_session(profile_name='iam-sync/bisc-freshchat/bisc-freshchat.IdP_read-only@klarna-data-production')
                #
                # s3 = boto3.resource('s3')
                #
                # copy_source = {'Bucket': bucket_name, 'Key': s3_source_path + file_name}
                # # s3.Object(bucket_name, s3_destination_path + file_name).copy(copy_source)
                # # s3.Object(bucket_name, s3_source_path + file_name).delete()
                #
                #
                #
                #
                # logging.info('file moved in S3 to: ' + s3_destination_path + file_name)
                #
                # bucket = s3.Bucket(bucket_name)
                # with open(local_source_file_path + file_name, 'wb') as write_file:
                #     bucket.download_fileobj(s3_destination_path + file_name, write_file)
                #
                # logging.info('file downloaded to ec2 at: ' + local_source_file_path + file_name)

        # except ClientError as e:
        #     if e.response['Error']['Code'] == '404':
        #         freshchat_daily.print_exception()
        #         logging.info(s3_source_path + file_name + " - is missing in S3!!")
        #     else:
        #         logging.exception("***************download_s3_files: something went wrong, see the traceback below....***************")
        #         raise




    def load_files_in_db(self):
        try:
            exit_code = 1

            logging.info('load_files_in_db: started for: ' + file_date)

            local_source_file_path = app.config.LOCAL_SOURCE_FILE_PATH

            # Tables for granular reports
            first_tmp_table = 'cs_raf.tmp_freshchat_file_dump_hourly'
            second_tmp_table = 'cs_raf.tmp_freshchat_raw_hourly'

            # Tables for the agents grouped reports
            first_tmp_table_agents = 'cs_raf.temp_freshchat_hourly_agent_file_dump'
            second_tmp_table_agents = 'cs_raf.tmp_freshchat_raw_hourly_agents'



            logging.info('local_source_file_path: ' + local_source_file_path)
            logging.info('looking for file_name_pattern: ' + file_name_pattern + file_name_pattern_agents)

            files_to_load = [f for f in os.listdir(local_source_file_path) if fnmatch.fnmatch(f, file_name_pattern)
                             or fnmatch.fnmatch(f, file_name_pattern_agents) ]

            logging.info('files found to load in database:')
            logging.info('We have ' + str(len(files_to_load)) +' files to load:')
            logging.info(files_to_load)

            if len(files_to_load) < 4:
                logging.info("no. of files found to be loaded is less than required. exiting the execution...")
                exit_code = -1
                raise SystemExit

            freshchat_daily.make_db_connection()

            # truncate the final target tmp table firstinsert_ts
            self.cursor.execute("truncate {};".format(second_tmp_table))
            self.cursor.execute("truncate {};".format(second_tmp_table_agents))
            logging.info('truncated: ' + second_tmp_table)
            logging.info('truncated: ' + second_tmp_table_agents)

            for new_files in os.listdir(local_source_file_path):

                if fnmatch.fnmatch(new_files, file_name_pattern):
                    self.cursor.execute("truncate {};".format(first_tmp_table))
                    logging.info('truncated: ' + first_tmp_table)
                    f = open(local_source_file_path + new_files, "r")
                    self.cursor.copy_expert("copy {} from stdin csv header quote '\"'".format(first_tmp_table), f) # copy data from file to pg tmp table
                    logging.info("Loaded "+new_files+" into {}".format(first_tmp_table))
                    self.cursor.callproc('cs_raf.ti_tmp_freshchat_raw_hourly', (new_files,))  # copy data from first tmp table to second tmp table
                    logging.info("Loaded "+new_files+" into {}".format(second_tmp_table))
                    f.close()
                    # os.remove(local_source_file_path + new_files)
                    # logging.info("Local copy deleted: "+new_files)
                    logging.info("TODO DENTRO")


                if fnmatch.fnmatch(new_files, file_name_pattern_agents):

                    self.cursor.execute("truncate {};".format(first_tmp_table_agents))
                    logging.info('truncated: ' + first_tmp_table)
                    f = open(local_source_file_path + new_files, "r")
                    self.cursor.copy_expert("copy {} from stdin csv header quote '\"'".format(first_tmp_table_agents), f) # copy data from file to pg tmp table
                    logging.info("Loaded "+new_files+" into {}".format(first_tmp_table))
                    self.cursor.callproc('cs_raf.ti_tmp_freshchat_raw_hourly_agents', (new_files,))  # copy data from first tmp table to second tmp table
                    logging.info("Loaded "+new_files+" into {}".format(second_tmp_table_agents))
                    f.close()
                    # os.remove(local_source_file_path + new_files)
                    # logging.info("Local copy deleted: "+new_files)
                    logging.info("TODO DENTRO")


        except SystemExit:
            logging.info('bye bye world!')
            return exit_code
        except:
            logging.exception("***************load_files_in_db: something went wrong, see the traceback below....***************")
            raise
        finally:
            if exit_code == 1:
                self.cursor.close()
                self.connection.close()
                logging.info("DB connection closed")
                logging.info('load_files_in_db: ended for: ' + file_date)
            return exit_code

    def process_data_in_db(self):
        try:
            logging.info('process_data_in_db: started for: ' + file_date)

            freshchat_daily.make_db_connection()

            subroutines_list = ['cs_raf.intraday_agent_performance_sykes',
                                'cs_raf.intraday_agent_performance_teleperformance',
                                'cs_raf.intraday_groupperformance_sykes',
                                'cs_raf.intraday_groupperformance_sykes_agg',
                                'cs_raf.intraday_groupperformance_teleperformance',
                                'cs_raf.intraday_groupperformance_teleperformance_agg',
                                'cs_raf.intraday_groupperformance_sykes_heimdall',
                                'cs_raf.intraday_groupperformance_teleperformance_heimdall'
                                ]

            for subroutine in subroutines_list:

                logging.info('process_data_in_db: %s started for: ' % subroutine + file_date)
                self.cursor.callproc(subroutine)

        except:
            logging.exception("***************process_data_in_db: something went wrong, see the traceback below....***************")

        finally:
            self.cursor.close()
            self.connection.close()
            logging.info("DB connection closed.")
            logging.info('process_data_in_db: ended for: ' + file_date)



if __name__ == '__main__':

    freshchat_daily = FreshchatDaily()
    # freshchat_daily.download_source_data_files()
    freshchat_daily.load_files_in_db()
    freshchat_daily.process_data_in_db()
    # freshchat_daily.download_source_data_files()
    # exit_code = freshchat_daily.load_files_in_db()
    # if exit_code == -1:  # if no. of files found < 2 then stop the FreshchatDaily()execution
    #     sys.exit()
    # freshchat_daily.process_data_in_db()
    # freshchat_daily.create_upload_supplier_files()

    logging.info('freshchat daily files - all done!!!')