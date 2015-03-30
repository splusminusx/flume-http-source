# Flume Http Source

Источник Flume событий с поддержкой [CORS](https://en.wikipedia.org/wiki/Cross-origin_resource_sharing)
(Cross-Origin Resource Sharing).

# Usage

Пример настройки агента в `flume.conf`:

```java
a1.sources = r1

a1.channels = c1
a1.channels.c1.type = memory

a1.sources.r1.type = ru.livetex.flume.HttpSource
a1.sources.r1.port = 5140
a1.sources.r1.channels = c1
a1.sources.r1.handler = ru.livetex.flume.HttpPayloadHandler
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = timestamp

a1.sinks = k1
a1.sinks.k1.channel = c1
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = hdfs://127.0.0.1:8020/flume/events/%Y-%m-%d/%H
a1.sinks.k1.hdfs.filePrefix = events-
a1.sinks.k1.hdfs.round = true
a1.sinks.k1.hdfs.roundValue = 10
a1.sinks.k1.hdfs.roundUnit = minute
```

Пример запуска Flume агента:

```bash
./flume-ng agent -f /etc/flume/flume.conf -n a1
```
