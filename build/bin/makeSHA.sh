#!/bin/sh

ls -1Ap | grep -v /\$ | grep -v .sha1 | while read -r line; do
  shasum $line | cut -c 1-40 > "$line.sha1"
done
