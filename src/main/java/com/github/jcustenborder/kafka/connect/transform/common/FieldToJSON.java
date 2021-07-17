/**
 * Copyright © 2020 Guillermo Rebaza (grebaza@gmail.com)
 * Adapted from com.github.jcustenborder.kafka.connect.transform.common.ToJSON
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.common;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import com.github.jcustenborder.kafka.common.cache.XSynchronizedCache;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.base.Charsets;

import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FieldToJSON<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(FieldToJSON.class);

  private static final Map<Schema.Type, Schema> FIELD_SCHEMA_MAPPING =
      new HashMap<>();
  private static final Map<Schema.Type, Function<byte[], ?>> FIELD_MAPPING_FUNC =
      new HashMap<>();

  // Handlers setup
  static {
    FIELD_SCHEMA_MAPPING.put(Schema.Type.STRING, Schema.OPTIONAL_STRING_SCHEMA);
    FIELD_SCHEMA_MAPPING.put(Schema.Type.BYTES, Schema.OPTIONAL_BYTES_SCHEMA);
    FIELD_MAPPING_FUNC.put(Schema.Type.STRING, a -> new String(a, Charsets.UTF_8));
    FIELD_MAPPING_FUNC.put(Schema.Type.BYTES, Function.identity());
  }

  FieldToJSONConfig config;
  //private Map<Schema, Schema> schemaCache;
  private XSynchronizedCache<Schema, Schema> schemaCache;

  JsonConverter converter = new JsonConverter();

  @Override
  public ConfigDef config() {
    return FieldToJSONConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new FieldToJSONConfig(settings);
    //this.schemaCache = new HashMap<>();
    this.schemaCache = new XSynchronizedCache<>(new LRUCache<>(16));

    // JsonConverter setup
    Map<String, Object> settingsClone = new LinkedHashMap<>(settings);
    settingsClone.put(FieldToJSONConfig.SCHEMAS_ENABLE_CONFIG,
                      this.config.schemasEnable);
    this.converter.configure(settingsClone, false);

  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    return schemaAndValue(inputSchema, input);
  }

  SchemaAndValue schemaAndValue(Schema inputSchema, Struct input) {
    final Schema outputSchema;
    final Struct outputValue;

    // Build output schema
    outputSchema = this.schemaCache.computeIfAbsent(inputSchema, s1 -> {
      final SchemaBuilder builder =
          SchemaUtil.copySchemaBasics(inputSchema, SchemaBuilder.struct());
      // input part
      for (Field field : inputSchema.fields()) {
        builder.field(field.name(), field.schema());
      }
      // converted fields
      for (Map.Entry<String, FieldToJSONConfig.FieldSettings>
          fieldSpec : this.config.conversions.entrySet()) {
        final FieldToJSONConfig.FieldSettings fieldSettings =
            fieldSpec.getValue();
        builder.field(
            fieldSettings.outputName,
            FIELD_SCHEMA_MAPPING.computeIfAbsent(
              fieldSettings.outputSchemaT, s2 -> {
                throw new UnsupportedOperationException(
                    String.format("Schema type '%s' is not supported.",
                      fieldSettings.outputSchemaT));
              })
        );
      }
      return builder.build();
    });

    // Build output value (input part)
    outputValue = new Struct(outputSchema);
    for (Field field : inputSchema.fields()) {
      final Object value = input.get(field);
      outputValue.put(field.name(), value);
    }

    // Build output value (converted fields)
    for (Map.Entry<String, FieldToJSONConfig.FieldSettings>
        fieldSpec : this.config.conversions.entrySet()) {
      final String field = fieldSpec.getKey();
      final FieldToJSONConfig.FieldSettings fieldSettings =
          fieldSpec.getValue();

      final Schema inputFieldSchema = input.schema().field(field).schema();
      final Object inputFieldValue = input.get(field);

      final Object convertedFieldValue;

      // convert to JSON
      final byte[] buffer = this.converter
        .fromConnectData("dummy", inputFieldSchema, inputFieldValue);

      convertedFieldValue = FIELD_MAPPING_FUNC.computeIfAbsent(
        fieldSettings.outputSchemaT, s -> {
          throw new UnsupportedOperationException(
              String.format("Schema type '%s' is not supported.",
                fieldSettings.outputSchemaT));
        }).apply(buffer);
      log.trace(String.format(
            "converted value: %s", convertedFieldValue.toString()));

      // update output value
      outputValue.put(fieldSettings.outputName, convertedFieldValue);
    }

    return new SchemaAndValue(outputSchema, outputValue);
  }

  @Title("FieldToJSON(Key)")
  @Description("This transformation is used to extract a field from a " +
               "nested struct convert into JSON string and append it to " +
               "the parent struct.")
  @DocumentationTip("This transformation is used to manipulate fields in " +
                    "the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends FieldToJSON<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.keySchema(), r.key());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          transformed.schema(),
          transformed.value(),
          r.valueSchema(),
          r.value(),
          r.timestamp()
          );
    }
  }

  @Title("FieldToJSON(Value)")
  @Description("This transformation is used to extract a field from a " +
               "nested struct convert into JSON string and append it to " +
               "the parent struct.")
  @DocumentationTip("This transformation is used to manipulate fields in " +
                    "the Value of the record.")
  public static class Value<R extends ConnectRecord<R>> extends FieldToJSON<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.valueSchema(), r.value());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          transformed.schema(),
          transformed.value(),
          r.timestamp()
          );
    }
  }

}
