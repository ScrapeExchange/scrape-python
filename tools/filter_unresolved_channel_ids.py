#!/usr/bin/env python3

import os
import sys
DATA_DIR = '../../byoda/data/scraped-channels'
UNRESOLVED_CHANNEL_ID_FILE = '../../byoda/data/unresolved_channel_ids.txt'
if os.path.exists(UNRESOLVED_CHANNEL_ID_FILE):
    with open(UNRESOLVED_CHANNEL_ID_FILE, 'r') as file_desc:
        for line in file_desc:
            line = line.strip()
            if line:
                open(
                    os.path.join(DATA_DIR, f'channel-{line}.unresolved'), 'w'
                ).close()

line: str
with open('/tmp/yt-channel.log', 'r') as file_desc:
    unresolved_channel_ids: set[str] = set()
    for line in file_desc:
        line = line.strip()
        if 'Failed to resolve channel ID' in line:
            channel_id: str = line.split(' ')[-1].strip()
            unresolved_file_path: str = os.path.join(
                DATA_DIR, f'channel-{channel_id}.unresolved'
            )
            if os.path.exists(unresolved_file_path):
                continue
            print('New unresolved channel ID:', channel_id)
            unresolved_channel_ids.add(channel_id)
            with open(unresolved_file_path, 'w') as unresolved_file:
                unresolved_file.write(f'{channel_id}\n')

print(f'Found {len(unresolved_channel_ids)} unique unresolved channel IDs.')

with open('../../byoda/data/channels.lst', 'r') as file_desc:
    existing_channels: set[str] = set()
    duplicate_channel_ids: int = 0
    duplicate_channel_names: int = 0
    channel_name: str
    for channel_name in file_desc:
        channel_name = channel_name.strip()
        if not channel_name:
            continue
        if ',' in channel_name:
            channel_name = channel_name.split(',', 1)[-1].strip()
        elif channel_name.startswith('uc') and len(channel_name) == 24:
            channel_name = 'UC' + channel_name[2:]

        if channel_name in unresolved_channel_ids:
            duplicate_channel_ids += 1
            continue
        if channel_name in existing_channels:
            duplicate_channel_names += 1
            continue
        existing_channels.add(channel_name)

print('Existing channels:', len(existing_channels))
print('Duplicate channel IDs:', duplicate_channel_ids)
print('Duplicate channel names:', duplicate_channel_names)

with open('/tmp/channels.lst', 'w') as file_desc:
    for channel_name in existing_channels:
        file_desc.write(f'{channel_name}\n')
