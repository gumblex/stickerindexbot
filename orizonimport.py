#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Import stickers from https://github.com/gumblex/orizonhub database.
'''

import sys
import json
import sqlite3
import stickerbot

db1 = sqlite3.connect(sys.argv[1])
cur1 = db1.cursor()
db2, _ = stickerbot.init_db(sys.argv[2])
cur2 = db2.cursor()

for row in cur1.execute("SELECT media FROM messages WHERE media LIKE '%sticker%' ORDER BY time ASC"):
    d = json.loads(row[0])
    sticker = d.get('sticker')
    if not sticker:
        continue
    cur2.execute('REPLACE INTO stickers VALUES (?,?,?,?,?)', (
        sticker['file_id'],
        sticker.get('width'),
        sticker.get('height'),
        sticker.get('emoji'),
        sticker.get('file_size')
    ))
db2.commit()
db2.execute('VACUUM')
