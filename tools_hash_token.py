#!/usr/bin/env python3
import hashlib
import sys

if len(sys.argv) != 2:
    print('Usage: python tools_hash_token.py <token>')
    raise SystemExit(1)
print(hashlib.sha256(sys.argv[1].encode('utf-8')).hexdigest())
