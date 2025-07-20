#!/usr/bin/env python3
# Compare the output of kafka-broker-api-versions for compatibility,
# specifically that the newer version is a superset of the older version

import re
import sys
from typing import TextIO

if len(sys.argv) != 3:
    print(f'usage: {sys.argv[0]} <older> <newer>')
    sys.exit(1)

def parse(file: TextIO) -> set[str]:
    heading = next(file)
    assert re.match(r'^[^:]+:\d+ \([^)]+\) -> \(\s*$', heading)

    versions_dict: dict[str, tuple[int] | tuple[int, int]] = {}
    for line in file:
        if line.strip() == ')': break

        m = re.match(r'^\s+([^:]+): (.*?),?\s*$', line)
        assert m is not None
        api_name = m.group(1)
        assert api_name not in versions_dict

        if m.group(2) == 'UNSUPPORTED': continue

        n = re.match(r'^(\d+)(?: to (\d+))? \[[^]]+\]$', m.group(2))
        assert n is not None
        versions_dict[api_name] = (int(n.group(1)), int(n.group(2))) \
            if n.lastindex == 2 else (int(n.group(1)),)

    return {
        f'{api_name}-{versions[0]}'
        for api_name, versions in versions_dict.items() if len(versions) == 1
    } | \
    {
        f'{api_name}-{version}'
        for api_name, versions in versions_dict.items() if len(versions) == 2
        for version in range(versions[0], versions[1]+1)
    }

with open(sys.argv[1]) as file1, open(sys.argv[2]) as file2:
    versions1, versions2 = parse(file1), parse(file2)

version_diffs: dict[str, set[int]] = {}
for version in versions1 - versions2:
    m = re.match(r'^(.*)-(\d+)$', version)
    assert m is not None
    if m.group(1) not in version_diffs:
        version_diffs[m.group(1)] = set()
    version_diffs[m.group(1)].add(int(m.group(2)))

api_names = sorted(
    list(version_diffs.keys()),
    key=lambda k: m.group(1) if (m := re.match(r'^[^(]+\((\d+)\)$', k)) else k
)
for api_name in api_names:
    print(f'{api_name}: {sorted(version_diffs[api_name])}')
