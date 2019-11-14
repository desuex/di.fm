import datetime
import requests
import sqlite3
import logging
import aiohttp
import asyncio
import async_timeout
from track_links import search
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-l", "--links", dest="parse_links", help="Parse YouTube links for tracks", default=False,
                  action="store_true")
parser.add_option("-a", "--artists", dest="parse_artists", help="Parse artists info", default=False,
                  action="store_true")
parser.add_option("-t", "--tracks", dest="parse_tracks", help="Parse tracks info", default=False,
                  action="store_true")
parser.add_option("-c", "--channels", dest="parse_channels", help="Parse channels info", default=False,
                  action="store_true")
parser.add_option("-d", "--debug", dest="debug_mode", help="Show debug information", default=False,
                  action="store_true")
parser.add_option("-i", "--init", dest="init_db", help="Create tables", default=False,
                  action="store_true")
parser.add_option("-w", "--watch", dest="watch_plays", help="Watch currently playing songs on all the channels",
                  default=False,
                  action="store_true")
options = parser.parse_args()[0]
parse_links = options.parse_links
parse_artists = options.parse_artists
parse_tracks = options.parse_tracks
parse_channels = options.parse_channels
init_db = options.init_db
watch_plays = options.watch_plays
debug_mode = options.debug_mode

if debug_mode:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

conn = sqlite3.connect('di-fm.db')
c = conn.cursor()

channel_url = 'https://www.di.fm/_papi/v1/di/channels/'
currently_playing_url = 'https://www.di.fm/_papi/v1/di/currently_playing'
track_url = 'https://www.di.fm/_papi/v1/di/tracks/'
artist_url = 'https://www.di.fm/_papi/v1/di/artists/'


def create_tables():
    logging.debug('Initializing the tables')
    c.execute(
        'CREATE TABLE IF NOT EXISTS channels (id integer NOT NULL, key text, name text, description text, asset_url '
        'text, banner_url text, PRIMARY KEY(id))')
    c.execute('CREATE TABLE IF NOT EXISTS artists (id integer NOT NULL, slug text, name text, bio_long text, bio_short '
              'text, PRIMARY KEY(id))')
    c.execute(
        'CREATE TABLE IF NOT EXISTS tracks (id integer NOT NULL, track text, title text, length integer, '
        'upvotes integer, downvotes integer, artist_id integer, default_image text, PRIMARY KEY(id))')
    c.execute('CREATE TABLE IF NOT EXISTS track_on_channel (channel_id integer NOT NULL, track_id integer NOT NULL, '
              'PRIMARY KEY (channel_id, track_id))')
    c.execute('CREATE TABLE IF NOT EXISTS artist_on_channel (channel_id integer NOT NULL, artist_id integer NOT NULL, '
              'PRIMARY KEY (channel_id, artist_id))')
    c.execute('CREATE TABLE IF NOT EXISTS artist_on_track (track_id integer NOT NULL, artist_id integer NOT NULL, '
              'PRIMARY KEY (track_id, artist_id))')
    c.execute('CREATE TABLE IF NOT EXISTS track_links (track_id integer NOT NULL, youtube_link text NOT NULL, '
              'PRIMARY KEY (track_id, youtube_link))')
    c.execute('CREATE TABLE IF NOT EXISTS track_link_attempts (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,'
              'track_id integer NOT NULL, attempt_at timestamp)')


def get_currently_playing():
    logging.info('Refreshing the current playing list')
    response = requests.get(currently_playing_url)
    data = list(map(lambda channel: (channel['channel_id'], channel['track']['id']), response.json()))
    logging.info('Result data length: %i' % len(data))
    logging.debug('Storing the current playing list to db')
    c.executemany('INSERT OR IGNORE INTO track_on_channel VALUES (?,?)', data)
    conn.commit()
    logging.debug('Done storing the current playing list to db')


async def background_link_search(queue_loop):
    logging.debug('Starting background link search')
    while True:
        for track_link in unparsed_track_links():
            print(track_link)
            logging.debug('Starting link parsing for track %s' % track_link[1])
            queue_loop.create_task(get_track_links(track_link[0], track_link[1]))
            await asyncio.sleep(1)
        await asyncio.sleep(60)


async def background_channels_update(queue_loop):
    async with aiohttp.ClientSession(loop=queue_loop) as session:
        tasks = [get_channel_info(session, channel_id) for channel_id in unparsed_channel_ids()]
        await asyncio.gather(*tasks)


async def background_tracks_update(queue_loop):
    while True:
        async with aiohttp.ClientSession(loop=queue_loop) as session:
            tasks = [get_track_info(session, track_id) for track_id in unparsed_track_ids()]
            await asyncio.gather(*tasks)
        await asyncio.sleep(60)


async def background_artists_update(queue_loop):
    while True:
        async with aiohttp.ClientSession(loop=queue_loop) as session:
            tasks = [get_artist_info(session, artist_id) for artist_id in unparsed_artist_ids()]
            tasks.extend(tasks)
            await asyncio.gather(*tasks)
        await asyncio.sleep(60)


async def check_channels_playing():
    while True:
        get_currently_playing()
        await asyncio.sleep(60)


def unparsed_artist_ids():
    data = []
    query_tracks = 'select distinct tracks.artist_id from tracks left join artists on tracks.artist_id = artists.id ' \
                   'where artists.id is null'
    for row in c.execute(query_tracks):
        data.append(row[0])
    query = 'SELECT DISTINCT aot.artist_id from artist_on_track aot LEFT JOIN artists a on a.id = aot.artist_id WHERE' \
            ' a.id IS NULL '

    for row in c.execute(query):
        data.append(row[0])
    return data


def unparsed_channel_ids():
    query = 'SELECT DISTINCT toc.channel_id from track_on_channel toc LEFT JOIN channels c on c.id = toc.channel_id ' \
            'WHERE c.id IS NULL'
    data = []
    for row in c.execute(query):
        data.append(row[0])
    return data


def unparsed_track_ids():
    query = 'SELECT DISTINCT toc.track_id from track_on_channel toc LEFT JOIN tracks t on t.id = toc.track_id WHERE ' \
            't.id IS NULL; '
    data = []
    for row in c.execute(query):
        data.append(row[0])
    return data


def unparsed_track_links():
    query = 'SELECT t.id as track_id, (a.name || " - " || t.title) as track_name from track_on_channel toc LEFT JOIN ' \
            'tracks t on t.id = toc.track_id LEFT JOIN artists a on a.id = t.artist_id LEFT JOIN track_link_attempts ' \
            'tla on tla.track_id = toc.track_id WHERE tla.attempt_at IS NULL'
    data = []
    for row in c.execute(query):
        data.append((row[0], row[1]))
    return data


async def get_artist_info(session, artist_id):
    url = "%s%i" % (artist_url, artist_id)
    await asyncio.sleep(1)
    try:
        with async_timeout.timeout(10):
            from aiohttp import ClientResponse
            response: ClientResponse
            async with session.get(url) as response:
                if response.status < 400:
                    json = await response.json()
                    logging.debug('Storing info about artists #%i "%s" ' % (artist_id, json['name']))
                    data = (artist_id, json['slug'], json['name'], json['bio_long'], json['bio_short'])
                    c.execute('INSERT OR IGNORE INTO artists VALUES (?,?,?,?,?)', data)
                conn.commit()
                return await response.release()
    except asyncio.TimeoutError as te:
        logging.error(te)
        return


async def get_track_info(session, track_id):
    url = "%s%i" % (track_url, track_id)
    await asyncio.sleep(1)
    try:
        with async_timeout.timeout(10):
            from aiohttp import ClientResponse
            response: ClientResponse
            async with session.get(url) as response:
                json = await response.json()
                logging.debug('Storing info about track #%i "%s" ' % (track_id, json['title']))
                data = (track_id, json['track'], json['title'], json.get('length', 0), json['votes'].get('up', '0'),
                        json['votes'].get('down', '0'), json['artist'].get('id', '0'),
                        json['images'].get("default", ""))
                c.execute('INSERT OR IGNORE INTO tracks VALUES (?,?,?,?,?,?,?,?)', data)
                conn.commit()

                data_artist = list(map(lambda artist: (track_id, artist['id']), json['artists']))
                logging.debug('Storing the artists from track to db')
                c.executemany('INSERT OR IGNORE INTO artist_on_track VALUES (?,?)', data_artist)
                conn.commit()

                return await response.release()
    except asyncio.TimeoutError as te:
        logging.error(te)
        return


async def get_channel_info(session, channel_id):
    url = "%s%i" % (channel_url, channel_id)
    await asyncio.sleep(1)
    try:
        with async_timeout.timeout(10):
            from aiohttp import ClientResponse
            response: ClientResponse
            async with session.get(url) as response:
                json = await response.json()
                logging.debug('Storing info about channel "%s"' % json['name'])
                data = (
                    channel_id, json['key'], json['name'], json['description'], json['asset_url'], json['banner_url'])
                c.execute('INSERT OR IGNORE INTO channels VALUES (?,?,?,?,?,?)', data)
                conn.commit()

                data_artist = list(map(lambda artist: (channel_id, artist['id']), json['artists']))
                logging.debug('Storing the artists from channel to db')
                c.executemany('INSERT OR IGNORE INTO artist_on_channel VALUES (?,?)', data_artist)
                conn.commit()

                return await response.release()
    except asyncio.TimeoutError as te:
        logging.error(te)
        return


async def get_track_links(track_id, track_name):
    logging.debug('Getting links for track #%i "%s"' % (track_id, track_name))
    query = 'INSERT INTO track_link_attempts (track_id, attempt_at)  VALUES (?, ?)'
    c.execute(query, (track_id, datetime.datetime.now()))
    conn.commit()
    search_res = search(track_name)
    results = list(map(lambda link: (track_id, link), search_res))
    logging.debug('Got %i results for track #%i "%s"' % (len(results), track_id, track_name))
    c.executemany('INSERT OR IGNORE INTO track_links VALUES (?,?)', results)
    conn.commit()


async def main(queue_loop):
    if parse_links:
        await background_link_search(queue_loop)
    if parse_artists:
        queue_loop.create_task(background_artists_update(queue_loop))
    if parse_channels:
        await background_channels_update(queue_loop)
    if parse_tracks:
        queue_loop.create_task(background_tracks_update(queue_loop))
    if watch_plays:
        await queue_loop.create_task(check_channels_playing())


if __name__ == '__main__':
    if init_db:
        create_tables()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    conn.close()
