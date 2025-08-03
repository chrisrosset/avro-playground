#!/usr/bin/env sh

rm -f *avro
rm -f *.avro.json

./main.py

echo "Raw sums:"
sha1sum *avro

echo
echo "Content sums:"
for x in *.avro; do
    ./main.py "$x" > "$x.json"
done

sha1sum *avro.json

echo
echo "File sizes:"
ls -la *avro
ls -la *avro.json
