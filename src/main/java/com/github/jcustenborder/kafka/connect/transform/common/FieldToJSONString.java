/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
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

import com.github.jcustenborder.kafka.common.cache.XSynchronizedCache;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class FieldToJSONString<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(FieldToJSONString.class);

  private static final Map<Schema.Type, Schema> schemaTHandler =
    new HashMap<>();
  private static final Map<Schema.Type, Function<byte[], ?>> valueHandler =
    new HashMap<>();

  private static final Function<byte[], ?> handleBytes = Function.identity();
  private static final Function<byte[], ?> handleString =
    a -> new String(a, Charsets.UTF_8);

  FieldToJSONStringConfig config;
  Map<Schema, Schema> schemaCache;
  //TRY-1: private Cache<Schema, Schema> schemaCache;

  JsonConverter converter = new JsonConverter();

  @Override
  public ConfigDef config() {
    return FieldToJSONStringConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new FieldToJSONStringConfig(settings);
    this.schemaCache = new HashMap<>();
    //TRY-1: this.schemaCache = new XSynchronizedCache<>(new LRUCache<>(16));

    // JsonConverter setup
    Map<String, Object> settingsClone = new LinkedHashMap<>(settings);
    settingsClone.put(FieldToJSONStringConfig.SCHEMAS_ENABLE_CONFIG,
                      this.config.schemasEnable);
    this.converter.configure(settingsClone, false);

    // Handlers setup
    schemaTHandler.put(Schema.Type.STRING, Schema.OPTIONAL_STRING_SCHEMA);
    schemaTHandler.put(Schema.Type.BYTES, Schema.OPTIONAL_BYTES_SCHEMA);
    valueHandler.put(Schema.Type.STRING, handleString);
    valueHandler.put(Schema.Type.BYTES, handleBytes);
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
      for (Map.Entry<String, FieldToJSONStringConfig.FieldSettings>
          fieldSpec : this.config.conversions.entrySet()) {
        final FieldToJSONStringConfig.FieldSettings fieldSettings =
          fieldSpec.getValue();
        builder.field(
          fieldSettings.outputName,
          schemaTHandler.computeIfAbsent(
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
    for (Map.Entry<String, FieldToJSONStringConfig.FieldSettings>
        fieldSpec : this.config.conversions.entrySet()) {
      final String field = fieldSpec.getKey();
      final FieldToJSONStringConfig.FieldSettings fieldSettings =
        fieldSpec.getValue();

      final Schema inputFieldSchema = input.schema().field(field).schema();
      final Object inputFieldValue = input.get(field);

      final Object convertedFieldValue;

      // convert to JSON
      final byte[] buffer = this.converter
        .fromConnectData("dummy", inputFieldSchema, inputFieldValue);

      convertedFieldValue = valueHandler.computeIfAbsent(
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

  @Title("FieldToJSONString(Key)")
  @Description("This transformation is used to extract a field from a "+
               "nested struct and append it to the parent struct in "+
               "JSON string.")
  @DocumentationTip("This transformation is used to manipulate fields in "+
                    "the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends FieldToJSONString<R> {

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

  @Title("FieldToJSONString(Value)")
  @Description("This transformation is used to extract a field from a " +
    "nested struct and append it to the parent struct in " +
    "JSON string.")
  public static class Value<R extends ConnectRecord<R>> extends FieldToJSONString<R> {

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
