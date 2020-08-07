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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
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

public abstract class FieldToJSONString<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(FieldToJSONString.class);

  FieldToJSONStringConfig config;
  Map<Schema, Schema> schemaCache;
  JsonConverter converter = new JsonConverter();

  @Override
  public ConfigDef config() {
    return FieldToJSONStringConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new FieldToJSONStringConfig(map);
    this.schemaCache = new HashMap<>();
    // schemaCache = new SynchronizedCache<>(new LRUCache<>(16));
    // JsonConverter setup
    Map<String, Object> settingsClone = new LinkedHashMap<>(map);
    settingsClone.put(FieldToJSONStringConfig.SCHEMAS_ENABLE_CONFIG,
                      this.config.schemasEnable);
    this.converter.configure(settingsClone, false);
  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    return schemaAndValue(inputSchema, input);
  }

  SchemaAndValue schemaAndValue(Schema inputSchema, Struct input) {
    // Input in Object and Struct fashion
    final Schema outputSchema;
    final Struct outputValue;

    final Schema inputFieldSchema = input.schema().field(this.config.inputFieldName).schema();
    final Object inputFieldValue = input.get(this.config.inputFieldName);

    final Schema convertedFieldSchema;
    final Object convertedFieldValue;

    // Convert to JSON
    final byte[] buffer = this.converter
      .fromConnectData("dummy", inputFieldSchema, inputFieldValue);

    switch (this.config.outputSchemaType) {
      case STRING:
        convertedFieldValue = new String(buffer, Charsets.UTF_8);
        convertedFieldSchema = Schema.OPTIONAL_STRING_SCHEMA;
        break;
      case BYTES:
        convertedFieldValue = buffer;
        convertedFieldSchema = Schema.OPTIONAL_BYTES_SCHEMA;
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Schema type (%s)'%s' is not supported.",
                FieldToJSONStringConfig.OUTPUT_SCHEMA_CONFIG,
                this.config.outputSchemaType
            )
        );
    }
    log.trace(String.format("value: %s", convertedFieldValue.toString()));

    // build output schema
    // outputSchema = outputSchemaCache.get(inputSchema);
    outputSchema = this.schemaCache.computeIfAbsent(inputSchema, s -> {
      final SchemaBuilder builder =
        SchemaUtil.copySchemaBasics(inputSchema, SchemaBuilder.struct());
      for (Field field : inputSchema.fields()) {
        builder.field(field.name(), field.schema());
      }
      builder.field(this.config.outputFieldName, convertedFieldSchema);
      return builder.build();
    });

    // outputSchemaCache.put(convertedFieldSchema, updatedSchema);

    // build output value
    outputValue = new Struct(outputSchema);
    for (Field field : inputSchema.fields()) {
      final Object value = input.get(field);
      outputValue.put(field.name(), value);
    }
    outputValue.put(this.config.outputFieldName, convertedFieldValue);

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
