# robin

Go implementation for [ 1 Billlion rows challenge](https://1brc.dev/) using  [pebble](https://github.com/cockroachdb/pebble) and serialized roaring bitmaps.


## Why ?

We use roaring bitmaps as storage format for  [vince](https://www.vinceanalytics.com/) - A self hosted alternative to Google analytics. We needed a sample application to validate our choice.

This challenge has very similar use case like vince, we have two columns, one for weather stations (with low cardinality) and another for values (with high cardinality).

In `vince` we store  low cardinality values ( like browser, os, os_version ... etc) which we use for filtering and high cardinality values like `duration` which we use for aggregation.

This package implements same ideas from vince with the goal of measuring storage cost and query performance (without any caching except on our code)

