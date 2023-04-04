#! env python

import pickle

import pandas as pd
import passpy
import requests as requests
from requests.adapters import HTTPAdapter, Retry

dirPickle = './in'
dirOut = './out'

store = passpy.Store()
TOKEN = store.get_key("ltu/figshare/prd/api_oauth_token").rstrip()

URL = "https://api.figshare.com/v2/{endpoint}"
url_stats_base = 'http://stats.figshare.com/latrobe/total/'
institution = 234
limit = 1000
xheaders = {"Authorization": "token " + TOKEN}
tbl = 'opal_articles'


def main():

    # API -> fn_out
    fn_out = f'{dirPickle}/{tbl}.txt'
    # ds = get_all_articles(params_extra, fn_out)

    # fn_in -> df
    fn_in = fn_out
    ds = read_json(fn_in)
    df = pd.DataFrame(ds)

    # df -> fn_out
    fn_out = f'{dirOut}/{tbl}.xlsx'
    df.to_excel(fn_out, index=False)

    # df -> fn_out (article counts)
    fn_out = f'{dirOut}/{tbl}_counts.xlsx'
    df = get_article_counts(df)
    df.to_excel(fn_out, index=False)

    # df -> df
    print(f'All done. See {fn_out}.')


# --- Get all public articles
#     - https://docs.figshare.com/#private_articles_search
def get_all_articles(params_extra={}, fn_out=None):
    endpoint = "articles/search"
    bPost = True
    ds = get_all_x_cursor(endpoint, params_extra, fn_out, bPost)
    return ds


# --- Get article-counts/stats for all articles in df
def get_article_counts(df):

    len_df = len(df)
    ds = []
    for index, row in df.iterrows():
        if index > 10:
            break
        aid = row['id']
        title = row['title']
        s = f'Getting stats for {index} of {len_df}: {aid} | {title}'
        print(s, end='')
        d = None
        try:
            d = get_article_count(aid)
            d['title'] = title
        except Exception as e:
            print(f'Failed to get stats for article with id {aid}: {e}')

        if d is not None:
            ds.append(d)
            n_downloads = d['downloads']
            n_views = d['views']
            n_shares = d['shares']
            s = f": Downloads:{n_downloads}|views:{n_views}|shares:{n_shares}"
            print(s)

    df = pd.DataFrame(ds)
    return df


# --- Get article-counts for article with id = aid
#     - https://docs.figshare.com/#stats_stats_service
def get_article_count(aid):

    types = ['downloads', 'views', 'shares']
    d = {}
    d['aid'] = aid
    for type in types:
        url = f'{url_stats_base}/{type}/article/{aid}'
        r = requests.get(url)
        resp = r.json()
        d[type] = resp['totals']
    # print(f'{url} -> {d}')
    return d


def read_json(fn):
    ds = []

    with open(fn, "rb") as f:
        while 1:
            try:
                ds.append(pickle.load(f))
            except EOFError:
                break
    return ds


# https://stackoverflow.com/questions/71177984/api-loop-update-get-request-with-next-cursor
def get_all_x_cursor(
    endpoint=None,
    params_extra=None,
    fn_out=None,
    bPost=True
        ):

    url = URL.format(endpoint=endpoint)
    offset = 0
    params = {
        "institution": institution,
        "limit": limit,
        "offset": offset,
    }
    if params_extra is not None:
        params.update(params_extra)
    MAX_RETRIES = 5
    BACKOFF_FACTOR = 0.1

    # Remove existing content
    with open(fn_out, "w") as fp:
        pass

    with requests.Session() as session:
        session.headers.update(xheaders)
        retry = Retry(
            total=MAX_RETRIES,
            status_forcelist=[500, 502, 503, 504],
            backoff_factor=BACKOFF_FACTOR,
        )
        session.mount(url, HTTPAdapter(max_retries=retry))
        i = 0
        total = 0
        bReturn = True
        ds = []

        bContinue = True
        while True:

            if bContinue is not True:
                break

            i += 1
            if bPost is True:
                (r := session.post(url, params=params)).raise_for_status()
            else:
                (r := session.get(url, params=params)).raise_for_status()

            try:
                xcursor = r.headers["X-Cursor"]
            except Exception:
                print("No X-Cursor -> Last iteration")
                bContinue = False

            if 'xcursor' in locals():
                headers = {"X-Cursor": xcursor}
                session.headers.update(headers)

            print(f"i: {i} | url: {url} | pms: {params}")
            ds = r.json()
            with open(fn_out, "ab+") as fp:
                for x in ds:
                    if type(x) is dict:
                        total += 1
                        # print(f'x -> {x} | type_x -> {type(x)}')
                        s = f"-> x:{x} with id:{x['id']}. ({i}:{total})"
                        print(s)
                        # print("---")
                        pickle.dump(x, fp)
                        if x["id"] in ds:
                            print("Already got this id. Aborting")
                            exit(1)
                        else:
                            ds.append(x["id"])

        if bReturn is True:
            print(f"Done. See {fn_out} and in opal.ds")

        return ds


if __name__ == '__main__':
    main()
