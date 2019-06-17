/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package de.fsteffen.kafana.kconnect.transform;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class InsertValueAsString<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Insert value as json String."
            + "<p/>Use the concrete transformation type designed for the record value (<code>"
            + Value.class.getName() + "</code>).";

    private static final JsonConverter JSON_CONVERTER = new JsonConverter();

    static {
        HashMap<String, Object> config = new HashMap<String, Object>(2);
        config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        config.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        JSON_CONVERTER.configure(config);
    }

    private interface ConfigName {
        String FIELD = "field";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(
            ConfigName.FIELD,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.MEDIUM,
            "Field name for record key."
    );

    private static final String PURPOSE = "value as json string insertion";

    private String field;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        field = config.getString(ConfigName.FIELD);

        if (field == null) {
            throw new ConfigException("No field for insertion configured");
        }

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(field, record.value().toString());
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        Schema schema = operatingSchema(record);
        if (schema.type().equals(Schema.Type.STRUCT)) {
            final Struct value = requireStruct(operatingValue(record), PURPOSE);

            Schema updatedSchema = schemaUpdateCache.get(value.schema());
            if (updatedSchema == null) {
                updatedSchema = makeUpdatedSchema(value.schema());
                schemaUpdateCache.put(value.schema(), updatedSchema);
            }

            final Struct updatedValue = new Struct(updatedSchema);

            for (Field field : value.schema().fields()) {
                updatedValue.put(field.name(), value.get(field));
            }

            byte[] parsedBytes = JSON_CONVERTER.fromConnectData(
                    record.topic(),
                    value.schema(),
                    value
            );
            updatedValue.put(field, new String(parsedBytes));

            return newRecord(record, updatedSchema, updatedValue);
        } else {
            return handlePrimitiveValue(record, schema);
        }
    }

    private R handlePrimitiveValue(R record, Schema schema) {
        final Object value = operatingValue(record);
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            updatedSchema = SchemaBuilder.struct()
                                         .field(field, schema)
                                         .build();
            schemaUpdateCache.put(schema, updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema).put(field, value);
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(field, Schema.STRING_SCHEMA);
        return builder.build();
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Value<R extends ConnectRecord<R>> extends InsertValueAsString<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updatedValue,
                    record.timestamp()
            );
        }

    }

}
