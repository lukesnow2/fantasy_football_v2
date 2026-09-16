#!/usr/bin/env python3
"""Sanitize a raw Yahoo data snapshot for public tracking.

Nulls fields classified by the Phase 0 privacy audit as secret or personal
with no pipeline consumer:

  - password                  league join password (credential)
  - short_invitation_url      league invitation link with embedded key (credential)
  - guid                      Yahoo account GUID (personal identifier)
  - is_owned_by_current_login account linkage flag
  - is_current_login          account linkage flag

Kept deliberately: manager nickname and image_url (consumed by dim_manager),
felo ratings, and all league/team/player data. See the audit for the full
classification.

Usage:
    python scripts/sanitize_snapshot.py <input.json> <output.json>

The walk is structure-preserving and deterministic: output differs from input
only in the values of the fields above. Writes atomically (temp + rename).
"""
import json
import os
import sys
import tempfile

SANITIZE_KEYS = frozenset({
    'password',
    'short_invitation_url',
    'guid',
    'is_owned_by_current_login',
    'is_current_login',
})


def sanitize(obj):
    """Recursively null sanitized keys in place; returns counts per key."""
    counts = {}

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k in SANITIZE_KEYS and v not in (None, ''):
                    node[k] = None
                    counts[k] = counts.get(k, 0) + 1
                else:
                    walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(obj)
    return counts


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    src, dst = sys.argv[1], sys.argv[2]

    with open(src) as f:
        data = json.load(f)

    counts = sanitize(data)

    # Atomic write: temp file in the destination directory, then rename.
    dst_dir = os.path.dirname(os.path.abspath(dst))
    fd, tmp = tempfile.mkstemp(dir=dst_dir, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(tmp, dst)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise

    total = sum(counts.values())
    print(f"Sanitized {total} values -> {dst}")
    for k in sorted(counts):
        print(f"  {k}: {counts[k]}")
    if total == 0:
        print("WARNING: nothing sanitized - wrong input file?")
        sys.exit(1)


if __name__ == '__main__':
    main()
