#!/usr/bin/env python3

line: str
with open('/tmp/yt-channel.log', 'r') as file_desc:
    channel_ids: set[str] = set()
    for line in file_desc:
        line = line.strip()
        if 'Failed to resolve channel ID' in line:
            channel_id: str = line.split(' ')[-1].strip()
            channel_ids.add(channel_id)


with open('../../byoda/data/channels.lst', 'r') as file_desc:
    existing_channels: set[str] = set()
    channel_name: str
    for line in file_desc:
        line = line.strip()
        if not line:
            continue
        if ',' in line:
            channel_name = line.split(',', 1)[-1].strip()
            existing_channels.add(channel_name)
        elif line.startswith('uc') and len(line) == 24:
            channel_name = 'UC' + line[2:]
            existing_channels.add(channel_name)
        else:
            existing_channels.add(line)


with open('/tmp/channels.lst', 'w') as file_desc:
    for channel_name in existing_channels:
        file_desc.write(f'{channel_name}\n')
