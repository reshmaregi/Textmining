#!/bin/bash

echo "Total disk space:"
du -sh

echo "PDF files:"
find . | grep -i ".pdf" | wc -l

echo "Tarballs:"
find . | grep -i -P ".(tgz|tar.gz|gz)" | wc -l

echo "Total files:"
find . |  wc -l
