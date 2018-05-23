"""
Программа эффективного вывода из моделей Seq2Seq

Выполнил: Емельяненко Дмитрий Викторович


Выполнено в рамках курсовой работы НИУ ВШЭ ФКН ПИ

2018
"""


from server import Server
from processor import *
import os
import configparser
from workers.batch_generation import TimeoutCostBatchGenerator, TimeoutBatchGenerator, NaiveBatchGenerator
import sys
from exceptions import MessagedException


def create_config(path):
    """
    Create a default file
    """
    config = configparser.ConfigParser()
    config.add_section('Settings')
    config.set('Settings', 'model_path', '')
    config.set('Settings', 'host', 'localhost')
    config.set('Settings', 'request_timeout', '10')
    config.set('Settings', 'max_query_len', '50')
    config.set('Settings', 'port', '2352')

    config.add_section('BatchGeneration')
    config.set('BatchGeneration', 'strategy', 'CostBased')
    config.set('BatchGeneration', 'batch_size', '64')
    config.set('BatchGeneration', 'batch_wait_timeout', '4')
    config.set('BatchGeneration', 'parallel_size', '1000')

    with open(path, 'w') as config_file:
        config.write(config_file)


def process_config(path):
    """
    processes config for testing
    """
    if not os.path.exists(path):
        create_config(path)

    config = configparser.ConfigParser()
    config.read(path)

    return config


try:
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'server.ini')

    is_read_conf = False
    for arg in sys.argv:
        if is_read_conf:
            config_path = arg
            is_read_conf = False
            break
        is_read_conf = arg == '--conf'

    assert not is_read_conf, MessagedException("No config file specified")
    assert os.path.isfile(config_path), MessagedException("Invalid path to configuration file")

    config_dir = os.path.dirname(os.path.abspath(config_path))

    config = process_config(config_path)

    def get_int(section, name):
        try:
            return int(config.get(section, name))
        except ValueError:
            raise MessagedException('{}/{} must be integer'.format(section, name))

    def get_float(section, name):
        try:
            return float(config.get(section, name))
        except ValueError:
            raise MessagedException('{}/{} must be a real value'.format(section, name))

    model_path = config.get('Settings', 'model_path')
    if not os.path.isabs(model_path):
        model_path = os.path.join(config_dir, model_path)
    host = config.get('Settings', 'host')
    port = get_int('Settings', 'port')
    assert port > 0, MessagedException("Port must be a positive integer")
    max_query_len = get_int('Settings', 'max_query_len')
    assert max_query_len > 0, MessagedException("max_query_len must be a positive integer")
    request_timeout = get_float('Settings', 'request_timeout')
    assert request_timeout > 0, MessagedException("request timeout must be positive")
    batch_generator_name = config.get('BatchGeneration', 'strategy')

    if batch_generator_name == 'CostBased':
        batch_size = get_int('BatchGeneration', 'batch_size')
        assert batch_size > 0, MessagedException("batch_size must be positive")

        timeout = get_float('BatchGeneration', 'batch_wait_timeout')
        assert timeout > 0, MessagedException("batch_wait_timeout must be positive")

        parallel_size = get_float('BatchGeneration', 'parallel_size')
        assert parallel_size > 0, MessagedException("Parallel size must be positive")
        batch_generator = TimeoutCostBatchGenerator(batch_size, timeout, parallel_size)
    elif batch_generator_name == 'Simple':
        batch_size = get_int('BatchGeneration', 'batch_size')
        assert batch_size > 0, MessagedException("batch_size must be positive")

        timeout = get_float('BatchGeneration', 'batch_wait_timeout')
        assert timeout > 0, MessagedException("batch_wait_timeout must be positive")

        batch_generator = TimeoutBatchGenerator(batch_size, timeout)
    elif batch_generator_name == 'Naive':
        batch_generator = NaiveBatchGenerator()
    else:
        raise MessagedException('unknown batch generation strategy')

    Server(BaseModelProcessor(batch_generator, model_path=model_path),
           host=host,
           port=port,
           timeout=request_timeout,
           max_query_len=max_query_len).run()
except MessagedException as e:
    print('Error:', e.message)
except Exception as e:
    print(sys.exc_info()[0].__name__ + '("{}")'.format(sys.exc_info()[1]))
