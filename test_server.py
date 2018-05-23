import aiohttp
import asyncio
import async_timeout
import random
import os
import sys
import configparser
import matplotlib.pyplot as plt
import numpy as np


def create_config(path):
    """
    Create a default file
    """
    default_config = configparser.ConfigParser()
    default_config.add_section('Settings')
    default_config.set('Settings', 'max_query_len', '20')
    default_config.set('Settings', 'query_template', 'http://localhost:2532/?query={}')
    default_config.set('Settings', 'request_timeout', '10')
    default_config.set('Settings', 'query_interval', '0.5')
    default_config.set('Settings', 'n_parallel_queries', '10')
    default_config.set('Settings', 'harden_interval', str(60 * 5))
    default_config.set('Settings', 'test_folder', 'test')
    default_config.set('Settings', 'test_name', 'test')
    default_config.set('Settings', 'percentiles', '[50, 90, 99]')
    default_config.set('Settings', 'plot_styles', '["-", "--", ":"]')
    default_config.set('Settings', 'plot_color', 'blue')
    default_config.set('Settings', 'plot_xmin', '0')
    default_config.set('Settings', 'plot_ymin', '0')
    default_config.set('Settings', 'plot_xmax', '150')
    default_config.set('Settings', 'plot_ymax', '5')
    default_config.set('Settings', 'query_len_probas',
                   '[0, 42, 213, 462, 699, 878, 982, 1014, 987, 916, 816, 702, 586, 475, 374, 288, 216, 158, 113, 79]')

    with open(path, 'w') as config_file:
        default_config.write(config_file)


def process_config(path):
    """
    processes config for testing
    """
    if not os.path.exists(path):
        create_config(path)

    config = configparser.ConfigParser()
    config.read(path)

    return config


config = process_config('default-test.ini')

max_query_len = int(config.get('Settings', 'max_query_len'))
assert max_query_len > 0

query_template = config.get('Settings', 'query_template')

request_timeout = float(config.get('Settings', 'request_timeout'))
assert request_timeout > 0

query_interval = float(config.get('Settings', 'query_interval'))
assert query_interval > 0

n_parallel_queries = int(config.get('Settings', 'n_parallel_queries'))
assert n_parallel_queries > 0

harden_interval = int(config.get('Settings', 'harden_interval'))
assert harden_interval > 0

tests_folder = config.get('Settings', 'test_folder')
test_name = config.get('Settings', 'test_name')

tests_folder = tests_folder if tests_folder.startswith('/') else os.path.join(*__file__.split('/')[:-1], tests_folder)

test_folder = os.path.join(tests_folder, test_name)

percentiles = eval(config.get('Settings', 'percentiles'))
assert type(percentiles) == list

query_len_probas = eval(config.get('Settings', 'query_len_probas'))
assert type(query_len_probas) == list
assert len(query_len_probas) == max_query_len

query_len_probas = np.array(query_len_probas)

plot_styles = eval(config.get('Settings', 'plot_styles'))
assert type(percentiles) == list

x_min = float(config.get('Settings', 'plot_xmin'))
y_min = float(config.get('Settings', 'plot_ymin'))
x_max = float(config.get('Settings', 'plot_xmax'))
y_max = float(config.get('Settings', 'plot_ymax'))

plot_color = config.get('Settings', 'plot_color')


delays = []
errored = False


async def fetch(session: aiohttp.ClientSession, url, loop):
    global request_timeout, errored
    try:
        async with async_timeout.timeout(request_timeout):
            time = loop.time()
            async with session.get(url) as response:
                return (await response.text()), loop.time() - time
    except:
        errored = True


async def generate_query():
    global query_template, max_query_len

    length = np.random.choice(np.arange(len(query_len_probas)), p=query_len_probas / query_len_probas.sum()) + 1
    return ''.join([chr(random.randint(0, ord('Z') - ord('A')) + ord('A')) for _ in range(length)])


async def query(loop):
    async with aiohttp.ClientSession() as session:
        query = await generate_query()
        result, time = await fetch(session, query_template.format(query), loop)
        delays.append(time)
        if query != result:
            print('Wrong result for {}: {}'.format(query, result), file=sys.stderr)

plots = [[] for _ in percentiles]
qps = []

if not os.path.isdir(test_folder):
    os.makedirs(test_folder)
print(test_folder)


async def main(loop):
    global n_parallel_queries, harden_interval, query_interval, percentiles
    time = loop.time()
    while True:
        queries = [query(loop) for _ in range(n_parallel_queries)]
        asyncio.ensure_future(asyncio.gather(*queries))
        await asyncio.sleep(query_interval)
        if loop.time() - time > harden_interval:
            current_qps = n_parallel_queries / query_interval
            qps.append(current_qps)

            fig = plt.figure()
            plt.hist(delays)
            plt.savefig(os.path.join(test_folder, 'hist_{}.png'.format(current_qps)))
            del fig
            fig = plt.figure()
            plt.xlim(x_min, x_max)
            plt.ylim(y_min, y_max)
            for i, percentile in enumerate(percentiles):
                value = np.percentile(delays, percentile, interpolation='higher')
                print(percentile, '\'th percentile', value)
                plots[i].append(value)
                plt.plot(qps, plots[i], color=plot_color, linestyle=plot_styles[i])
            plt.savefig(os.path.join(test_folder, 'plot.png'))
            del fig
            print('=' * 50)
            time = loop.time()
            delays.clear()

            print('Done with', current_qps)
            n_parallel_queries += 1
            if errored:
                break

loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))
