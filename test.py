import urllib.request
import os
import time
import re
from io import StringIO

import pandas as pd
from datetime import datetime, timedelta

PROJ_DIR = '/home/harlan/Projects/data/'


def get_html(url: str, cache_file: str = None, timeout: int = -1) -> str:
 
    read_cache_file = False
    if cache_file and os.path.exists(cache_file) and timeout > 0:
        mod_delta = time.time() - os.stat(cache_file).st_mtime
        read_cache_file = mod_delta > timeout

    if not read_cache_file:
        bytes = urllib.request.urlopen(url).read()
        html = bytes.decode('utf8')

        if cache_file:
            with open(cache_file, 'w') as f:
                f.write(html)
    else:
        with open(cache_file, 'r') as f:
            html = f.read()

    return(html)


def get_anchored_string(bufr: str, anchor: str, end_tag: str = '') -> str:
    pat = f'{anchor}(.*?){end_tag}'
    res = re.search(pat, bufr, re.MULTILINE | re.DOTALL)
    return res.group(1) if res else None


def process_volatility(bufr: str) -> pd.DataFrame:

    # <pre> </pre>
    anchors = ['$BKX', 'ZYXI']
    # we lose anchors[1] row
    bufr = get_anchored_string(bufr or '', f'\\{anchors[0]}', anchors[1])
    if not bufr:
        return None

    hdr = 'symbol hv20 hv50 hv100 date curiv days percentile close'
    bufr = hdr + '\n' + anchors[0] + bufr

    # tweaks
    bufr = bufr.replace('/100%ile', '/ 100%ile')
    bufr = bufr.replace('%ile', '%ile ')
    bufr = bufr.replace('SERIAL OPTION', '0 0 0')
    zero = '0.00     0/  0%ile'
    bufr = bufr.replace(zero, '010101 ' + zero)

    converters = {
        'date': lambda x: datetime.strptime(x,'%y%m%d'),
        'days': lambda x: int(x.strip('/')),
        'percentile': lambda x: int(x[:-4]),
    }

    df_vol = pd.read_csv(
        StringIO(bufr),
        delim_whitespace=True,
        comment='*',
        converters=converters,
    )

    return df_vol


def process_muni(bufr: str) -> pd.DataFrame:

    pat = 'property="article:modified_time" content="(.+)T'
    as_of = pd.Timestamp(re.search(pat, bufr).group(1))

    anchors = ['>AAA RATED MUNI BONDS</', 'screen-reader-response']
    bufr = get_anchored_string(bufr or '', f'\\{anchors[0]}', anchors[1])
    if not bufr:
        return None

    pat = r'<tr><td>national</td><td>([0123]+) year</td><td>([.0-9]+)</td><td>([.0-9]+)</td></tr>'
    ratings = re.findall(pat, bufr)
    assert len(ratings) == 9

    df_muni = pd.DataFrame({
        'rating': 3 * ['AAA'] + 3 * ['AA'] + 3 * ['A'],
    })
    df_muni[['term', 'yield', 'yield_last']] = pd.DataFrame(ratings).apply(pd.to_numeric)

    return df_muni


if __name__ == '__main__':
    url = 'https://www.optionstrategist.com/calculators/free-volatility-data'
    html = get_html(url, PROJ_DIR + 'vol.txt', 20 * 3600)
    df_vol = process_volatility(html)

    url = 'https://www.fmsbonds.com/market-yields/'
    html = get_html(url, PROJ_DIR + 'muni.txt', 20 * 3600)
    df_muni = process_muni(html)

    print('done!')