import time
from fabric.api import *

from create_files import FILES


HADOOP_HOST = 'sgoeddel@127.0.0.1:8000'
HADOOP_HOME = '/home/sgoeddel/hadoop'
TEST_DIR = '/tmp'
FILES_DIR = 'testFiles/'
HADOOP_COMMAND = 'bin/hadoop fs'
RUN_TIMES = 2

env.hosts = [HADOOP_HOST]

def test_performance(start_and_stop=False):
    if start_and_stop:
        start_hdfs()

    with cd(HADOOP_HOME):
        make_test_dir()
        time_adding_empty_dir()
        time_adding_empty_file()
        time_putting_all_files()
        # time_reading_all_files()
        time_listing_directory()
        remove_test_dir()

    if start_and_stop:
        stop_hdfs()


def time_adding_empty_dir():
    total = 0
    for i in range(0, RUN_TIMES, 1):
        start = time.clock()
        add_dir('directory')
        end = time.clock()
        total += end - start
        remove_dir('directory')

    print 'Adding empty dir: ' + unicode(total/RUN_TIMES) + ' seconds; averaged over ' + unicode(RUN_TIMES) + ' times.'


def time_adding_empty_file():
    total = 0
    for i in range(0, RUN_TIMES, 1):
        start = time.clock()
        add_file('file.txt')
        end = time.clock()
        total += end - start
        remove_file('file.txt')

    print 'Adding empty file: ' + unicode(total / RUN_TIMES) + ' seconds; averaged over ' + unicode(RUN_TIMES) + ' times.'


def time_putting_all_files():
    for key, value in FILES.iteritems():
        total = 0
        for i in range(0, RUN_TIMES, 1):
            start = time.clock()
            put_file(FILES_DIR + key + '.txt')
            end = time.clock()
            total += end - start
            if i < RUN_TIMES - 1:
                remove_file(key + '.txt')

        print 'Adding ' + key + ': ' + unicode(total / RUN_TIMES) + ' seconds; averaged over ' + unicode(RUN_TIMES) + ' times.'


def time_reading_all_files():
    for key, value in FILES.iteritems():
        total = 0
        for i in range(0, RUN_TIMES, 1):
            start = time.clock()
            read_file(key + '.txt')
            end = time.clock()
            total += end - start

        print 'Reading ' + key + ': ' + unicode(total / RUN_TIMES) + ' seconds; averaged over ' + unicode(RUN_TIMES) + ' times.'


def time_listing_directory():
    total = 0
    for i in range(0, RUN_TIMES, 1):
        start = time.clock()
        run(HADOOP_COMMAND + ' -ls /tmp')
        end = time.clock()
        total += end - start

    print 'listing /tmp dir: ' + unicode(total / RUN_TIMES) + ' seconds; averaged over ' + unicode(RUN_TIMES) + ' times.'


def start_hdfs():
    with cd(HADOOP_HOME):
        run('sbin/start-dfs.sh')


def stop_hdfs():
    with cd(HADOOP_HOME):
        run('sbin/stop-dfs.sh')


def make_test_dir():
    add_dir()


def remove_test_dir():
    remove_dir()


def dir(dir_name=None, add=True):
    with cd(HADOOP_HOME):
        suffix = ''
        if dir_name is not None:
            suffix = '/' + dir_name

        command = '-rm -r'
        if add:
            command = '-mkdir'

        run(HADOOP_COMMAND + ' ' + command + ' ' + TEST_DIR + suffix)


def add_dir(dir_name=None):
    dir(dir_name, True)


def remove_dir(dir_name=None):
    dir(dir_name, False)


def file(file_name, add=True):
    with cd(HADOOP_HOME):
        suffix = '/' + file_name

        command = '-rm'
        if add:
            command = '-touchz'

        run(HADOOP_COMMAND + ' ' + command + ' ' + TEST_DIR + suffix)


def add_file(file_name):
    dir(file_name, True)


def remove_file(file_name):
    dir(file_name, False)


def put_file(file_name):
    with cd(HADOOP_HOME):
        run(HADOOP_COMMAND + ' -copyFromLocal -f ' + file_name + ' ' + TEST_DIR + '/' + file_name.replace(FILES_DIR, ''))


def read_file(file_name):
    with cd(HADOOP_HOME):
        run(HADOOP_COMMAND + ' -cat ' + TEST_DIR + '/' + file_name)


def copy_files_to_server():
    with cd(HADOOP_HOME):
        for key, value in FILES.iteritems():
            file_name = key + '.txt'
            put('../' + file_name, FILES_DIR + file_name)