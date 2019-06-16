# Kinspector

1. Build jar with custom transformation `mvn clean package`
2. Start components `docker-compose up` and wait until they are up
3. Add schema to registry via maven registry plugin `mvn schema-registry:register -N`
4. Produce records via rest-proxy from [here](./connect-es-kibana.http)
5. Add es-kafka-connect via http request from [here](./connect-es-kibana.http#L32)
6. Goto [kibana](http://localhost:5601) create an index-pattern and inspect records

## TODO

* delete records
* multiple topics
* primitive message values
* add screens
