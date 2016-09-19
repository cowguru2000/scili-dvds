from bs4 import BeautifulSoup
import argparse
import os
import psycopg2
import requests
import re
import sys
import time

CATALOG_URL = "http://library.brown.edu/collatoz/videos.php?task=loc&location=Friedman"
TMDB_URL = "http://api.themoviedb.org/3"
#TMDB_URL = "http://private-anon-b3a2c031c4-themoviedb.apiary-mock.com/3"
TMDB_RATE_LIMIT = 40 # requests per...
TMDB_LIMIT_WINDOW = 10 # seconds

class RateLimiter:
    def __init__(self, window_secs, max_reqs):
        self.frames = []
        self.window_secs = window_secs
        self.max_reqs = max_reqs
        self.total = 0

    def bump(self):
        self.total += 1
        f = len(self.frames)
        now = int(time.time())
        if f == 0 or now > self.frames[0]['time']:
            self.frames.insert(0, {'time': now, 'hits': 1})
            while self.frames[f]['time'] < now - self.window_secs:
                self.total -= self.frames.pop()['hits']
                f -= 1
        else:
            self.frames[0]['hits'] += 1

        return self.total >= self.max_reqs

class TMDB:
    def __init__(self, api_key):
        self.api_key = api_key
        self.rl = RateLimiter(TMDB_LIMIT_WINDOW, TMDB_RATE_LIMIT)

    def request(self, endpoint, addtl_params = {}):
        params = {'api_key': self.api_key}
        params.update(addtl_params)
        if self.rl.bump():
            time.sleep(TMDB_LIMIT_WINDOW)
        return requests.get(
            TMDB_URL + endpoint,
            params=params
        )

def main():
    # we need the API key from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument('api_key', type=str, help='Your TMDB API key')
    parser.add_argument('postgresql_url', type=str, help='PostgreSQL URI (e.g. postgresql://user:pass@host/database)')
    parser.add_argument('--static-path', type=str, default='static/posters/')
    parser.add_argument('--pdb', type=str, default='scilidvd', help='PostgreSQL database name')
    parser.add_argument('--puser', type=str, default='scilidvd', help='PostgreSQL user')
    parser.add_argument('--ppass', type=str, help='PostgreSQL password')
    parser.add_argument('--phost', type=str, default='localhost', help='PostgreSQL host')
    parser.add_argument('--pport', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--verbose', type=bool, default=False)
    args = parser.parse_args()

    if args.ppass is None:
        print("Must provide a PostgreSQL password")
        sys.exit(1)

    def v(msg):
        if args.verbose:
            print(msg)

    conn = psycopg2.connect(database=args.pdb, user=args.puser, password=args.ppass, host=args.phost, port=args.pport)
    cur = conn.cursor()
    # prepare select statement
    cur.execute(
        "prepare sel_movie as "
        "SELECT * FROM movies WHERE josiah_callno = $1")

    t = TMDB(args.api_key)
    # grab TMDB configuration info for building poster paths
    tmdb_data = t.request("/configuration").json()

    preferred_widths = ['w342', 'w500', 'w780', 'original']
    width = None
    for preferred_width in preferred_widths:
        if preferred_width in tmdb_data['images']['poster_sizes']:
            width = preferred_width
            break

    if width is None:
        print("No preferred poster width found. Options are:")
        for w in tmdb_data['images']['poster_sizes']:
            print('- ' + w)
        return

    imagepath_fmt = (
        tmdb_data['images']['base_url'] + 'w300' + "%s"
    )

    markup = requests.get(CATALOG_URL).text
    soup = BeautifulSoup(markup, "lxml")

    # scrape the SciLi DVD collection page... fun
    table = soup.find('table')
    rows = table.find_all('tr')[1:]
    for row in rows:
        # parse each movie

        cells = row.find_all('td')
        title = re.search(r'>([^<]+)', str(cells[0].b)).group(1)
        callno = re.search(r'record=([a-zA-Z0-9]+)$', cells[2].a['href'])

        if callno is None:
            print("No call number found for %s" % title)
            continue
        else:
            callno = callno.group(1)

        # see if we already have a record for this movie
        cur.execute("EXECUTE sel_movie (%s)", [callno])
        poster_filepath = args.static_path + '/%s.jpg' % callno
        if cur.rowcount == 0 or not os.path.isfile(poster_filepath):
            # hit TMDB for movie info
            try:
                tmdb_data = t.request('/search/movie',
                        {'query': title}).json()
                if 'total_results' not in tmdb_data:
                    print("Error while fetching from TMDB:")
                    print(tmdb_data)
                    return
                if tmdb_data['total_results'] == 0:
                    print("No results found for \"%s\"" % title)
                    continue
            except Exception as e:
                print("Couldn't get search results for %s" % title)
                print(e)
                continue

            movie_data = tmdb_data['results'][0]
            record = {
                'title': movie_data['title'],
                'plot_short': movie_data['overview'],
                'josiah_callno': callno
            }
            movie_id = movie_data['id']
            full_poster_path = imagepath_fmt % movie_data['poster_path']

            if cur.rowcount == 0:
                # hit TMDB details endpoint for more info incl. runtime
                try:
                    more_info = t.request('/movie/%s' % movie_id,
                        {'append_to_response': 'credits'}).json()

                    record['runtime'] = int(more_info['runtime'])

                    # find director in credits
                    for person in more_info['credits']['crew']:
                        if person['job'] == 'Director':
                            record['director'] = person['name']
                            break

                except Exception as e:
                    print("Couldn't get details for %s" % record['title'])
                    print(e)
                    continue

                # create record
                print("Creating record for %s..." % record['title'])
                pairs = record.items()
                qstr = ("INSERT INTO movies "
                    '(' + ','.join(map(lambda x: x[0], pairs)) + ')'
                    ' VALUES '
                    '(' + ','.join(["%s"] * len(pairs)) + ')')
                cur.execute(qstr, map(lambda x: x[1], pairs))

        else:  # cur.rowcount != 0 and we already have poster
            v("Already have entry and poster for movie %s" % title)

        # check to see if we already have poster
        if not os.path.isfile(poster_filepath):
            # download poster
            print('\tDownloading poster for %s' % title)
            poster = requests.get(full_poster_path)
            with open(poster_filepath, 'wb') as fd:
                for chunk in poster.iter_content(1024):
                    fd.write(chunk)

    conn.commit()

    cur.close()
    conn.close()

    print("%d movies processed." % len(rows))

if __name__ == "__main__":
    main()
