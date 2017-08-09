# flight-datastore-diplomat

datastore module for getto/flight by peburrows/diplomat

## usage

### find

```
docker run getto/flight-datastore-diplomat find <kind> --file data.json

# data.json
{"key": <key>, "conditions": {"col": "val"}, "columns": ["col"]}
```

## pull

```
docker pull getto/flight-datastore-diplomat
```

## build

```
docker build -t getto/flight-datastore-diplomat
```
