package de.fsteffen.kinspector.convert;

import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import io.confluent.connect.avro.AvroConverter;

public class TombstoneAwareAvroConverter implements Converter {

    private AvroConverter avroConverter;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        avroConverter = new AvroConverter();
        avroConverter.configure(configs, isKey);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return avroConverter.fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        SchemaAndValue tombstone = new SchemaAndValue(Schema.STRING_SCHEMA, "TOMBSTONE");
        return Optional.of(avroConverter.toConnectData(topic, value))
                       .filter(v -> !SchemaAndValue.NULL.equals(v))
                       .orElseGet(() -> tombstone);
    }

}
