from fabric.api import *

HADOOP_HOST = 'sgoeddel@127.0.0.1:8000'
HADOOP_HOME = '/home/sgoeddel/hadoop'
TEST_DIR = '/tmp'
HADOOP_COMMAND = 'bin/hadoop fs'

env.hosts = [HADOOP_HOST]

def test_performance(start_and_stop=False):
    if start_and_stop:
        start_hdfs()

    with cd(HADOOP_HOME):
        make_test_dir()
        add_dir('directory')
        add_file('file.txt')
        run(HADOOP_COMMAND + ' -ls /tmp')
        remove_file('file.txt')
        remove_test_dir()

    if start_and_stop:
        stop_hdfs()


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