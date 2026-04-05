#!/usr/bin/env python3

line: str
with open('/tmp/yt-channel.log', 'r') as file_desc:
    channel_ids: set[str] = set()
    for line in file_desc:
        line = line.strip()
        if 'Failed to resolve channel ID' in line:
            channel_id: str = line.split(' ')[-1].strip()
            print('Unresolved channel ID:', channel_id)
            channel_ids.add(channel_id)

print(f'Found {len(channel_ids)} unique unresolved channel IDs.')

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

        if channel_name in channel_ids:
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
