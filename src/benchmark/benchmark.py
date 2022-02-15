#!/bin/env python

import csv, sys, json, os, traceback, logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import threading
import requests

TEST_HOME = os.path.dirname(os.path.realpath(__file__)) + "/../"
_ITER_SLEEP_TIME = 2

# Conf file reader
def read_config(conf_file):
    fp = open(conf_file, 'r')
    conf = json.load(fp=fp)
    logging.info("Config passed = " + str(conf))
    return conf

# Gets SQLs configuration
def get_sqls_conf(conf_file):
    conf = read_config(conf_file)
    return conf["sqls"]

# Params parser
def get_params(conf_file):
    conf = read_config(conf_file)
    return conf["cluster"], conf["arguments"], conf["method"]

# Generic switch implementation for all conf files
def get_conf(option):
    return {
        "test" : TEST_HOME + "conf/test_conf.json"
    }.get(option, TEST_HOME + "conf/user_histogram.json")

# Helps determine the user histogram using the conf file
def get_thread_histogram(users, conf):
    hist_json = read_config(conf)
    return ( int(float(hist_json["reads"]) / float(100) * float(users)) ), \
           ( int(float(hist_json["appends"]) / float(100) * float(users)) ), \
           ( int(float(hist_json["updates"]) / float(100) * float(users)) ), \
           ( int(float(hist_json["deletes"]) / float(100) * float(users)) )

def mkdirp(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)

# Reporter
def write_report_header(report_file_path):
    f = open(report_file_path, 'a')
    writer = csv.writer(f)
    try:
        writer.writerow( ('Thread ID', 'Engine', 'Category', 'Start Time', 'End Time', 'Duration') )
    finally:
        f.close()

def write_report(report_file_path, category, engine, thread_id, st_time, end_time):
    logging.info("Writing report to file - %s " % report_file_path)
    f = open(report_file_path, 'a')
    try:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        elapsed = end_time - st_time
        writer.writerow( (thread_id, engine, category, st_time, end_time, elapsed) )
    finally:
        f.close()
    logging.info("Completed the write of report to file - %s" % report_file_path)


def run_final_all_verification():
    pass

def run_verification(category, test_conf):
    pass

# Invoke the query 
def run_query(engine_conf, category_conf):

    # Read the engine configs

    # Read the category configs

    # 

    if ()



# Function that runs the test in an executor thread. Each thread runs the same test in both Synapse and ADB Spark clusters
def run_test(category, thread_id, test_conf, iters, base_report_dir):
    report_file_path = base_report_dir + "/" + category + "_" + threading.current_thread().getName() + '_report.csv'
    write_report_header(report_file_path)

    # Run in Synapse Spark
    for i in range(0, iters):
        st_time = int(time.time())
        sc = run_query(test_conf["engine"]["synapse"], test_conf[category])
        end_time = int(time.time())

        if sc.status != "done":
            logging.info("Failure seen with " + str(sc.id))
            return -1
        else:
            logging.debug("All done with thread %s " + thread_id)
            write_report(report_file_path, category, "synapse", thread_id, st_time, end_time)
            return 0

    run_verification(category, test_conf)

    # Run in ADB Spark clusters
    for i in range(0, iters):
        st_time = int(time.time())
        sc = run_query(test_conf["engine"]["adb"], test_conf[category])
        end_time = int(time.time())

        if sc.status != "done":
            logging.info("Failure seen with " + str(sc.id))
            return -1
        else:
            logging.debug("All done with thread %s " + thread_id)
            write_report(report_file_path, category, "adb", thread_id, st_time, end_time)
            return 0
    
    run_verification(category)



# main workhorse which will scaffold the running of the tests
def execute(options):
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(thread)d %(message)s',
                    filename = TEST_HOME + '/logs/delta_test.log',
                    filemode='w')

    iters = options.iters
    if iters == 0:
        logging.error("Iterations should at least be 1. Terminating!")
        return

    # Prepare the report dir
    current_date_time = time.strftime("%Y-%m-%d-%H-%M-%S")
    base_report_dir = TEST_HOME + '/reports/' + "report_" + current_date_time + "/"
    mkdirp(base_report_dir)

    # Read the config files
    threads_read, threads_append, threads_update, threads_delete = get_thread_histogram(int(options.users), get_conf("users"))
    test_conf = get_conf("test")

    futures = []

    # Make things concurrent now. Implement thread pool
    with ThreadPoolExecutor(max_workers=options.users) as executor:
        for i in range(0, threads_read):
            rd_thr_id = "rd_" + str(i)
            futures.append(executor.submit(run_test, "reads", rd_thr_id, test_conf, options.iters, base_report_dir))
            time.sleep(_ITER_SLEEP_TIME)

        for i in range(0, threads_append):
            app_thr_id = "app_" + str(i)
            futures.append(executor.submit(run_test, "appends", app_thr_id, test_conf, options.iters, base_report_dir))
            time.sleep(_ITER_SLEEP_TIME)

        for i in range(0, threads_update):
            up_thr_id = "upd_" + str(i)
            futures.append(executor.submit(run_test, "updates", up_thr_id, test_conf, options.iters, base_report_dir))
            time.sleep(_ITER_SLEEP_TIME)

        for i in range(0, threads_delete):
            del_thr_id = "del_" + str(i)
            futures.append(executor.submit(run_test, "deletes", del_thr_id, test_conf, options.iters, base_report_dir))
            time.sleep(_ITER_SLEEP_TIME)

    result_dict = {}
    if len(futures) > 0:
        for f in as_completed(futures):
            result_dict.update(f.result())
    logging.info(str(result_dict))
    
    run_final_all_verification()

    logging.info("Ending the test now! Thank you.")
    return 0

# Main
if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        requiredNamed = parser.add_argument_group('Required arguments')
        requiredNamed.add_argument("-u", "--users", help="The number of concurrent users", required=True)
        parser.add_argument("-i", "--iters", help="The number of iterations each test should be run", default=1)
        options = parser.parse_args()
        sys.exit(execute(options))
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(3)