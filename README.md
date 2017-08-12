# flight-datastore-diplomat

datastore module for getto/flight by peburrows/diplomat

## usage

### find

```
echo $data
# => {
  "key": <key>,
  "conditions": {"col": "val"},
  "columns": ["col"]
}

docker run \
  -e FLIGHT_DATA="$data" \
  -e GCP_CREDENTIALS_JSON="$json" \
  getto/flight-datastore-diplomat \
  flight_datastore find <kind>

# => {"col": <val>}
```

### modify

```
echo $data
# => {
  "operator": <operator>,
  "data": [
    {
      "action": "insert",
      "kind": <kind>,
      "properties": {"col": "val"}
    },
    {
      "action": "replace",
      "kind": <kind>,
      "key": <key>,
      "old-key": <old-key>,
      "properties": {"col": "val"}
    },
    {
      "action": "update"|"upsert",
      "kind": <kind>,
      "key": <key>,
      "properties": {"col": "val"}
    },
    {
      "action": "delete",
      "kind": <kind>,
      "key": <key>,
    }
  ]
}

docker run \
  -e FLIGHT_DATA="$data" \
  -e GCP_CREDENTIALS_JSON="$json" \
  getto/flight-datastore-diplomat \
  flight_datastore modify <kind> [<kind>...]

# => {"col": <val>}
```

## pull

```
docker pull getto/flight-datastore-diplomat
```

## build

```
docker build -t getto/flight-datastore-diplomat
```
