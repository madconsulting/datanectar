import os
import sys
import json
import pathlib
import logging
import luigi
import luigi.contrib.s3 as luigi_s3

# Basic config
DATANECTAR_CONFIG_FILENAME = 'datanectar.json'
DATANECTAR_LOG_FILENAME = 'datanectar.log'
DATANECTAR_LOG_PATH = os.path.abspath(DATANECTAR_LOG_FILENAME)

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # TODO: Set to critical
file_log_handler = logging.FileHandler(DATANECTAR_LOG_PATH)
logger.addHandler(file_log_handler)
logger.addHandler(logging.StreamHandler(sys.stdout))


def get_datanectar_config(root):
    root_path = pathlib.Path(root) / DATANECTAR_CONFIG_FILENAME
    if root_path.exists():
        with open(root_path, 'r') as f:
            return json.load(f)
    else:
        return {}


def get_task_files(root):
    task_list = []
    for p in pathlib.Path(root).glob('*/*task.py'):
        task_list.append(p)
    return task_list


def get_task_names_in_root(root):
    task_paths = get_task_files(root)
    task_list = []
    for task_path in task_paths:
        task_path_str = str(task_path)
        if task_path_str.lower().endswith('.py'):
            task_name = task_path_str[:-len('.py')]
            if task_name.startswith(root):
                task_name = task_name[len(str(root) + '/'):]
            task_list.append(task_name)
    return task_list


def get_task_name(task_obj):
    """eta/extract_task.py -> 'eta/extract_task"""
    #print(f'get_task_name({task_obj}')
    #print('.file = {}'.format(task_obj.__module__))
    #if isinstance(task_file_or_obj, luigi.Task):
    module_str = task_obj.__module__
    return module_str.replace('.', '/')


def get_output_dir(root, task_obj, output_type=None):
    config = get_datanectar_config(root)
    if not output_type:
        output_type = config.get('output_type', 'local')

    task_name = get_task_name(task_obj)

    if output_type == 'local':
        return get_task_name(task_obj)
    elif output_type == 's3':
        bucket = config.get('bucket', 'datanectar')
        return f's3://{bucket}/{task_name}'


def get_datanectar_env_vars():
    datanectar_env_vars = {}
    for env_var_name, env_var_value in os.environ.items():
        if env_var_name.startswith('DN_'):
            datanectar_env_vars[env_var_name] = env_var_value
    return datanectar_env_vars


def get_s3_client():
    dn_env_vars = get_datanectar_env_vars()
    if 'DN_AWS_ACCESS_KEY_ID' in dn_env_vars and 'DN_AWS_SECRET_ACCESS_KEY' in dn_env_vars:
        aws_access_key = dn_env_vars['DN_AWS_ACCESS_KEY_ID']
        aws_secret_key = dn_env_vars['DN_AWS_SECRET_ACCESS_KEY']
        s3client = luigi_s3.S3Client(aws_access_key, aws_secret_key)
        return s3client


def get_luigi_output_target(root, task_obj, output_type=None, filename=None):
    output_path = get_output_dir(root, task_obj, output_type)
    if filename:
        output_path = output_path / filename

    if not output_type:
        config = get_datanectar_config(root)
        if 'output_type' in config:
            output_type = config['output_type']
        else:
            logger.log(f'(Root={root}) No output_type specified - using default: "local"')
            output_type = 'local'

    if output_type == 'local':
        return luigi.LocalTarget(output_path)
    elif output_type == 's3':
        return luigi_s3.S3Target(output_path, client=get_s3_client())


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--root', action='store', default='test_root')
    args = parser.parse_args()
    root = args.root

    print(f'Datanectar root: {root}')

    dn_config = get_datanectar_config(root)
    print(f'\nDatanectar config (in root={root}): {dn_config}')

    task_files = get_task_files(root)
    print(f'\nTask files (in root={root}): {task_files}')

    task_list = get_task_names_in_root(root)
    print(f'\nTask names (in root={root}): {task_list}')

    # Testing calling datanectar and passing in Luigi task
    if root == 'test_root':
        sys.path.append(root)
        import etl.extract_task
        import etl.rollup_task
        task1 = etl.extract_task.ExtractTask()
        task2 = etl.rollup_task.RollupTask()
        output_dir = get_output_dir(root, task1)
        print(f'output_dir for {task1}: {output_dir}')
        output_dir2 = get_output_dir(root, task2)
        print(f'output_dir for {task2}: {output_dir2}')
