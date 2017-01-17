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

Sticker = collections.namedtuple('Sticker', 'file_id width height emoji file_size')

class AttrDict(dict):

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

class BotAPIFailed(Exception):
    pass

class SQLiteStateStore(collections.UserDict):
    TABLE = 'bot_status'

    def __init__(self, connection):
        self.conn = connection
        data = {k: json.loads(v) for k,v in cur.execute('SELECT key, value FROM ' + TABLE)}
        super().__init__(data)

    def commit(self):
        with self.lock:
            cur = self.conn.cursor()
            for k, v in self.data.items():
                cur.execute('REPLACE INTO %s (key, value) VALUES (?,?)' % TABLE,
                            (k, json.dumps(v)))
            self.conn.commit()

    def close(self):
        self.commit()

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
def sendmsg(text, chat_id, reply_to_message_id=None):
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
    return bot_api('sendMessage', chat_id=chat_id, text=text, reply_to_message_id=reply_id)

@async_func
def answer(inline_query_id, results, **kwargs):
    return bot_api('answerInlineQuery', inline_query_id=inline_query_id,
                   results=json.dumps(results), **kwargs)

def getupdates():
    global CFG
    while 1:
        try:
            updates = bot_api('getUpdates', offset=CFG['offset'], timeout=10)
        except Exception:
            logger_botapi.exception('Get updates failed.')
            continue
        if updates:
            CFG['offset'] = updates[-1]["update_id"] + 1
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
    if len(cmd) > 1 and 'username' in CFG and cmd[-1] != CFG['username']:
        return (None, None)
    expr = t[1] if len(t) > 1 else ''
    return (cmd[0][1:], expr.strip())

def init_db():
    global DB, CONN, STATE
    DB = sqlite3.connect(CFG['database'])
    DB.row_factory = sqlite3.Row
    CONN = DB.cursor()
    CONN.execute('CREATE TABLE IF NOT EXISTS stickers ('
        'file_id TEXT PRIMARY KEY,'
        'width INTEGER,'
        'height Integer,'
        'emoji TEXT,'
        'file_size INTEGER'
    ')')
    # should have some special tags
    CONN.execute('CREATE TABLE IF NOT EXISTS tags ('
        'sticker TEXT,'
        'tag TEXT,'
        'PRIMARY KEY (sticker, tag),'
        'FOREIGN KEY (sticker) REFERENCES stickers(file_id)'
    ')')
    CONN.execute('CREATE TABLE IF NOT EXISTS packs ('
        'identifier TEXT PRIMARY KEY,'
        'name TEXT'
    ')')
    CONN.execute('CREATE TABLE IF NOT EXISTS sticker_pack ('
        'sticker TEXT,'
        'pack TEXT,'
        'PRIMARY KEY (sticker, pack),'
        'FOREIGN KEY (sticker) REFERENCES stickers(file_id),'
        'FOREIGN KEY (pack) REFERENCES sticker_pack(identifier)'
    ')')
    CONN.execute('CREATE TABLE IF NOT EXISTS admins ('
        'user INTEGER PRIMARY KEY,'
        'added_by INTEGER'
    ')')
    CONN.execute('CREATE TABLE IF NOT EXISTS bot_status ('
        'key TEXT PRIMARY KEY,'
        'value TEXT'
    ')')
    # may be deleted
    CONN.execute('CREATE TABLE IF NOT EXISTS op_log ('
        'user INTEGER,'
        'time INTEGER,'
        'op TEXT,'
        'sticker TEXT,'
        'tag TEXT,'
        'pack TEXT'
    ')')
    CONN.execute('CREATE INDEX IF NOT EXISTS idx_emoji ON stickers (emoji)')
    CONN.execute('CREATE INDEX IF NOT EXISTS idx_tags ON tags (tag)')
    STATE = SQLiteStateStore(CONN)

def handle_api_update(d: dict):
    logger_botapi.debug('Update: %r' % d)
    try:
        if 'inline_query' in d:
            query = d['inline_query']
            text = query['query'].strip()
            if text:
                stickers = get_sticker(text)
                r = answer(query['id'], inline_result(stickers))
                logger_botapi.debug(r)
        elif 'message' in d:
            msg = d['message']
            text = msg.get('text', '')
            cmd, expr = parse_cmd(text)
            ret = None
            if not expr and not text:
                pass
            elif cmd == 'start':
                ret = 'Send me ...'
            elif cmd == 'help':
                ret = '...'
            elif cmd == 'car' or msg['chat']['type'] == 'private':
                ret = format_result(*cmd_query(expr or text))
            if ret:
                bot_api('sendMessage', chat_id=msg['chat']['id'], text=ret,
                    reply_to_message_id=msg['message_id'], parse_mode='Markdown')
    except Exception:
        logger_botapi.exception('Failed to process a message.')

def inline_result(stickers):
    if not details:
        return []
    ret = []
    for d in stickers:
        ret.append({
            'type': 'sticker',
            'id': d.file_id,
            'sticker_file_id': d.file_id
        })
    return ret

def get_sticker(text, num=5):
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
            tags.extend(x.lstrip('#') for x in frag.strip().split())
    if emojis:
        where = ' OR '.join('emoji = ?' for x in emojis)
        vals = emojis
    if tags:
        if where:
            where = '(%s) AND ' % where
        where += ' AND '.join('t.tag = ?' for x in tags)
        vals.extend(tags)
    return [Sticker(file_id, None, None, emoji, None) for file_id, emoji in
            CONN.execute('SELECT file_id, emoji FROM stickers WHERE ' + where, vals)]

def on_text(expr, chatid, replyid, msg):
    ...

def load_config():
    return AttrDict(json.load(open('config.json', encoding='utf-8')))

if __name__ == '__main__':
    CFG = load_config()
    MSG_Q = queue.Queue()
    DB = CONN = STATE = None
    try:
        apithr = threading.Thread(target=getupdates)
        apithr.daemon = True
        apithr.start()

        while 1:
            handle_api_update(MSG_Q.get())
    finally:
        STATE.close()
