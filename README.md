# Kafana

At some point e.g. when debugging kafka streams applications, kafka data needs to be inspected.
There are some tools like `kafkacat` and also some UIs helping us to do this.
Especially with the combination of `kafkacat` + `jq` it's possible to search and filter for everything you need.
But is it convenient enough and do you want to consume all records again and again for each query?

The idea is to make use of existing tools to inspect kafka data instead of building another UI.
In details it's using kafka connect with some transformations to sink messages from kafka topics to Elasticsearch.
Transformations are mostly used to add record meta data as separate fields to indexed data.
IDs are composed by topic + partition + offset to have rather a change log than a table.
Tombstones are specially treated as synthetic string message ("TOMBSTONE") to have them in the log as well.
Data in Elasticsearch should reflect state in kafka as close as possible.
To prevent huge indices its might be useful to define some rollover conditions.
After sinking the data to an index, kibana can be used for discover, visualizing, and searching for messages.

## How to run

1. Build jar with custom transformation `mvn clean package`
2. Start components `docker-compose up` and wait until they are up
3. Add schema to registry via maven registry plugin `mvn schema-registry:register -N`
4. Produce records via rest-proxy from [here](./kafana.http)
5. Add es-kafka-connect via http request from [here](./kafana.http#L63)
6. Goto [kibana](http://localhost:5601) create an index-pattern and inspect records

## Screens

### Discover messages

![discover](/attachments/discover.png)

### Search for key across topics

![search-for-key](/attachments/search_for_key.png)

### Search for key and topic

![search-for-key-and-topic](/attachments/search_for_key_and_topic.png)

### Search for message field across topics

![search-for-field-across-topics](/attachments/search_for_message_field.png)
