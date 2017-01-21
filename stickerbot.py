#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Telegram Sticker Index Bot
'''

import re
import sys
import time
import json
import queue
import sqlite3
import logging
import requests
import functools
import threading
import collections
import concurrent.futures

import zhconv

logging.basicConfig(stream=sys.stderr, format='%(asctime)s [%(name)s:%(levelname)s] %(message)s', level=logging.DEBUG if sys.argv[-1] == '-v' else logging.INFO)

logger_botapi = logging.getLogger('botapi')

executor = concurrent.futures.ThreadPoolExecutor(5)
HSession = requests.Session()

_re_one_emoji = (
    '[🇦-🇿]|'
    '(?:(?:[🌀-🏺]|[🐀-🙏]|[🚀-\U0001f6ff]|[\U0001f900-\U0001f9ff])[🏻-🏿]?\u200d)*'
    '(?:[🌀-🏺]|[🐀-🙏]|[🚀-\U0001f6ff]|[\U0001f900-\U0001f9ff])[🏻-🏿]?'
)

re_emoji = re.compile('(%s)' % _re_one_emoji)
re_qtag = re.compile(r"#?\w+", re.UNICODE)
re_tag = re.compile(r"#\w+", re.UNICODE)
re_tags = re.compile(r"#\w+(?:\s+#\w+)*", re.UNICODE)

class Sticker(collections.namedtuple('Sticker', 'file_id width height emoji file_size')):
    @classmethod
    def from_telegram(cls, sticker):
        return cls(
            sticker['file_id'],
            sticker['width'],
            sticker['height'],
            sticker.get('emoji'),
            sticker.get('file_size')
        )

class AttrDict(dict):

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

class SQLiteStateStore(collections.UserDict):
    TABLE = 'bot_status'

    def __init__(self, connection):
        self.conn = connection
        cur = self.conn.cursor()
        data = {k: json.loads(v) for k,v in cur.execute(
                'SELECT key, value FROM ' + self.TABLE)}
        super().__init__(data)

    def commit(self):
        cur = self.conn.cursor()
        for k, v in self.data.items():
            cur.execute('REPLACE INTO %s (key, value) VALUES (?,?)' % self.TABLE,
                        (k, json.dumps(v)))
        self.conn.commit()

    def close(self):
        self.commit()

def nt_from_dict(nt, d, default=None):
    kwargs = dict.fromkeys(nt._fields, default)
    kwargs.update(d)
    return nt(**kwargs)

# Bot API

class BotAPIFailed(Exception):
    pass

def async_func(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        def func_noerr(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception:
                logger_botapi.exception('Async function failed.')
        executor.submit(func_noerr, *args, **kwargs)
    return wrapped

def bot_api(method, **params):
    for att in range(3):
        try:
            req = HSession.post(('https://api.telegram.org/bot%s/' %
                                CFG.apitoken) + method, data=params, timeout=45)
            retjson = req.content
            ret = json.loads(retjson.decode('utf-8'))
            break
        except Exception as ex:
            if att < 1:
                time.sleep((att + 1) * 2)
            else:
                raise ex
    if not ret['ok']:
        raise BotAPIFailed(repr(ret))
    return ret['result']

@async_func
def sendmsg(text, chat_id, reply_to_message_id=None, **kwargs):
    text = text.strip()
    if not text:
        logger_botapi.warning('Empty message ignored: %s, %s' % (chat_id, reply_to_message_id))
        return
    logger_botapi.debug('sendMessage(%s): %s' % (len(text), text[:20]))
    if len(text) > 2000:
        text = text[:1999] + '…'
    reply_id = reply_to_message_id
    if reply_to_message_id and reply_to_message_id < 0:
        reply_id = None
    return bot_api('sendMessage', chat_id=chat_id, text=text,
                   reply_to_message_id=reply_id, **kwargs)

@async_func
def answer(inline_query_id, results, **kwargs):
    return bot_api('answerInlineQuery', inline_query_id=inline_query_id,
                   results=json.dumps(results), **kwargs)

def getupdates():
    global CFG, STATE
    while 1:
        try:
            updates = bot_api('getUpdates', offset=STATE.get('offset', 0), timeout=10)
        except Exception:
            logger_botapi.exception('Get updates failed.')
            continue
        if updates:
            STATE['offset'] = updates[-1]["update_id"] + 1
            for upd in updates:
                MSG_Q.put(upd)
        time.sleep(.2)

def parse_cmd(text: str):
    t = text.strip().replace('\xa0', ' ').split(' ', 1)
    if not t:
        return (None, None)
    cmd = t[0].rsplit('@', 1)
    if len(cmd[0]) < 2 or cmd[0][0] != "/":
        return (None, None)
    if len(cmd) > 1 and 'username' in CFG and cmd[-1] != CFG.username:
        return (None, None)
    expr = t[1] if len(t) > 1 else ''
    return (cmd[0][1:], expr.strip())

# DB stuff

def init_db(filename):
    db = sqlite3.connect(filename)
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS stickers ('
        'file_id TEXT PRIMARY KEY,'
        'width INTEGER,'
        'height Integer,'
        'emoji TEXT,'
        'file_size INTEGER'
    ')')
    # should have some special tags
    cur.execute('CREATE TABLE IF NOT EXISTS tags ('
        'sticker TEXT,'
        'tag TEXT,'
        'PRIMARY KEY (sticker, tag),'
        'FOREIGN KEY (sticker) REFERENCES stickers(file_id)'
    ')')
    cur.execute('CREATE TABLE IF NOT EXISTS tag_index ('
        'tag TEXT PRIMARY KEY,'
        'indexed TEXT,'
        'FOREIGN KEY (tag) REFERENCES tags(tag)'
    ')')
    cur.execute('CREATE TABLE IF NOT EXISTS bot_status ('
        'key TEXT PRIMARY KEY,'
        'value TEXT'
    ')')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_emoji ON stickers (emoji)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_tags ON tags (sticker, tag)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_tag_index ON tag_index (indexed)')
    db.commit()
    state = SQLiteStateStore(db)
    return db, state

@functools.lru_cache(maxsize=64)
def normalize_tag(tag):
    '''
    Normalize tag to be indexable
    `tag` must be \w+
    '''
    indexedtag = tag.lower().replace('_', '')
    return zhconv.convert(indexedtag, 'zh-hans')

def add_sticker(sticker, tags=None):
    cur = DB.cursor()
    if isinstance(sticker, Sticker):
        cur.execute('INSERT OR IGNORE INTO stickers VALUES (?,?,?,?,?)', sticker)
        sticker_id = sticker.file_id
        emoji = sticker.emoji
    else:
        sticker_id = sticker
        emoji = '[?]'
    if tags:
        logging.info('Tags %r added for %s %s', tags, emoji, sticker_id)
        for tag in tags:
            indexed = normalize_tag(tag)
            cur.execute('INSERT OR IGNORE INTO tags VALUES (?,?)', (sticker_id, tag))
            cur.execute('INSERT OR IGNORE INTO tag_index VALUES (?,?)', (tag, indexed))
    else:
        logging.debug('Seen %s %s', emoji, sticker_id)
    DB.commit()

def del_tag(sticker, tag):
    if isinstance(sticker, Sticker):
        sticker_id = sticker.file_id
    else:
        sticker_id = sticker
    cur = DB.cursor()
    cur.execute('DELETE FROM tags WHERE sticker=? AND tag=?', (sticker_id, tag))
    DB.commit()

def vacuum_db():
    cur = DB.cursor()
    cur.execute('DELETE FROM tag_index WHERE tag NOT IN (SELECT DISTINCT tag from tags)')
    DB.commit()
    DB.vacuum()

def get_sticker(text, num=50):
    text = text.strip()
    if not text:
        return []
    emojis = []
    tags = []
    where, vals = '', []
    for frag in re_emoji.split(text):
        if re_emoji.match(frag):
            emojis.append(frag)
        else:
            tags.extend(x.lstrip('#') for x in frag.strip().split() if re_qtag.match(x))
    join = ''
    if emojis:
        where = ' OR '.join('emoji = ?' for x in emojis)
        vals = emojis.copy()
    if tags:
        if where:
            where = '(%s) AND ' % where
        where += ' AND '.join('t.indexed = ?' for x in tags)
        join = (' LEFT JOIN tags ON tags.sticker = stickers.file_id'
                ' LEFT JOIN tag_index t ON t.tag = tags.tag')
        vals.extend(normalize_tag(t) for t in tags)
    if not where:
        return []
    sql = 'SELECT file_id, emoji FROM stickers%s WHERE %s LIMIT ?' % (join, where)
    vals.append(num)
    cur = DB.cursor()
    ret = [Sticker(file_id, None, None, emoji, None) for file_id, emoji in
           cur.execute(sql, vals)]
    if not ret and tags:
        where = ' OR '.join('t.indexed LIKE ?' for x in tags)
        vals = ['%' + normalize_tag(t) + '%' for t in tags]
        vals.append(num)
        sql = 'SELECT file_id, emoji FROM stickers%s WHERE %s LIMIT ?' % (join, where)
        ret = [Sticker(file_id, None, None, emoji, None) for file_id, emoji in
               cur.execute(sql, vals)]
    return ret

# Query handling

START = 'This is the Sticker Index Bot. Send /help, or directly use its inline mode.'

HELP = ('You can search for stickers by tags or emoji in its inline mode.\n'
'This bot will collect tags for stickers in groups or private chat, '
'after seeing stickers being replied to in the format "#tagone #tagtwo".'
)

def handle_api_update(d: dict):
    logger_botapi.debug('Update: %r' % d)
    try:
        if 'inline_query' in d:
            query = d['inline_query']
            text = query['query'].strip()
            if text:
                stickers = get_sticker(text)
                logging.info('Got %d stickers for %s', len(stickers), text)
                r = answer(query['id'], inline_result(stickers))
                logger_botapi.debug(r)
        elif 'message' in d:
            msg = d['message']
            text = msg.get('text', '')
            sticker = msg.get('sticker')
            ret = None
            if sticker:
                on_sticker(sticker, msg['chat'], msg)
            elif text:
                cmd, expr = parse_cmd(text)
                if not cmd:
                    ret = on_text(text, msg['chat'], msg['message_id'], msg)
                elif msg['chat']['type'] == 'private':
                    msgchatid = str(msg['chat']['id'])
                    if msgchatid in STATE:
                        STATE[msgchatid] = None
                    if cmd == 'start':
                        ret = START
                    # elif cmd == 'help':
                    else:
                        ret = HELP
            if ret:
                sendmsg(ret, msg['chat']['id'], msg['message_id'])
    except Exception:
        logger_botapi.exception('Failed to process a message.')

def inline_result(stickers):
    ret = []
    for d in stickers:
        ret.append({
            'type': 'sticker',
            'id': d.file_id,
            'sticker_file_id': d.file_id
        })
    return ret

def on_text(text, chat, replyid, msg):
    if not re_tags.match(text):
        if chat['type'] == 'private':
            return 'Please send me a sticker and its tag(s).'
    if 'reply_to_message' in msg and 'sticker' in msg['reply_to_message']:
        if chat['type'] != 'private':
            match = re_tags.match(text.strip())
            if match:
                tags = [x.lstrip('#') for x in match.group(0).split()]
        else:
            tags = [x.lstrip('#') for x in text.strip().split() if re_qtag.match(x)]
        add_sticker(Sticker.from_telegram(msg['reply_to_message']['sticker']), tags)
        if chat['type'] == 'private':
            return 'Tags added.'
    elif chat['type'] == 'private':
        sticker = STATE.get(str(chat['id']))
        if sticker:
            tags = [x.lstrip('#') for x in text.strip().split() if re_qtag.match(x)]
            add_sticker(sticker, tags)
            STATE[str(chat['id'])] = None
            return 'Tags added.'

def on_sticker(sticker, chat, msg):
    sticker_obj = Sticker.from_telegram(sticker)
    add_sticker(sticker_obj)
    if chat['type'] == 'private':
        STATE[str(chat['id'])] = sticker_obj.file_id

def load_config():
    return AttrDict(json.load(open('config.json', encoding='utf-8')))

if __name__ == '__main__':
    CFG = load_config()
    MSG_Q = queue.Queue()
    DB, STATE = init_db(CFG.database)
    try:
        apithr = threading.Thread(target=getupdates)
        apithr.daemon = True
        apithr.start()
        logging.info('Satellite launched')
        while 1:
            handle_api_update(MSG_Q.get())
    finally:
        STATE.close()
