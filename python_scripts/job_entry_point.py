import importlib
import argparse
import time
import datetime
import logging 
import os
import sys

logger = logging.getLogger(__name__)




if __name__ == '__main__':

    try:
        topic=sys.argv[1]
        broker=sys.argv[2]
        job_name=sys.argv[3]
        window_streaming_seconds=sys.argv[4]
        args={}

        args['job_name']=job_name
        args['broker']=broker
        args['topic']=topic
        args['window_streaming_seconds']=window_streaming_seconds

        if job_name=='jobs.stream_spark_df':
            output_format=sys.argv[5]
            args['output_format']=output_format
        
        logging.info(f'job_name set to {job_name}'.format(job_name))
    except IndexError as error:
        logging.error('----------------------------------------------------------------')
        logging.error('You need to specify arguments at runtime.')
        logging.error('Something like :')
        logging.error('----------------------------------------------------------------')


    start = time.time()

    # configure logging format for root logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s {app_name} %(name)s %(message)s'.format(app_name=job_name)
    )

    try:
        # Run job
        module = importlib.import_module(job_name)
        module.main(args)
        end = time.time()
        logger.info("Execution of job %s took %s seconds" % (job_name, end-start), 
                    {'job_duration':end-start})
    
    except Exception as e:

        logger.error(str(datetime.datetime.now()) + "Job failed")

        raise Exception("Exception::Job %s failed with msg %s" %(job_name, str(e)))