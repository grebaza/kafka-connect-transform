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

  @Override
  public ConfigDef config() {
    return FieldToJSONStringConfig.config();
  }

  @Override
  public void close() {

  }

  JsonConverter converter = new JsonConverter();

  @Override
  public void configure(Map<String, ?> map) {
    this.config = new FieldToJSONStringConfig(map);
    this.schemaCache = new HashMap<>(); //TODO: Review
    // JsonConverter setup
    Map<String, Object> settingsClone = new LinkedHashMap<>(settings);
    settingsClone.put(FieldToJSONStringConfig.SCHEMAS_ENABLE_CONFIG,
        this.config.schemasEnable);
    this.converter.configure(settingsClone, false);
  }

  SchemaAndValue schemaAndValue(Schema inputSchema, Object input) {
    //TODO
    // Input in Object and Struct fashion
    final Object inputFieldObject = input.get(this.config.inputFieldName);
    final Struct inputFieldStruct = input.getStruct(this.config.inputFieldName);

    // Schema
    final Schema outputSchema = this.schemaCache
      .computeIfAbsent(inputSchema, s -> {
        final Field inputField = inputSchema.field(this.config.inputFieldName);
        final SchemaBuilder builder = SchemaBuilder.struct();
        for (Field elementField : inputSchema.fields()) {
          builder.field(elementField.name(), elementField.schema());
        }
        builder.field(this.config.inputFieldName, inputField.schema());
        return builder.build();
      }
    );

    // Value
    // Creating outputStruct (all values of struct)
    final Struct outputStruct = new Struct(outputSchema);
    for (Field inputField : inputSchema.fields()) {
      final Object value = input.get(inputField);
      outputStruct.put(inputField.name(), value);
    }
    // Adding the input in JSON string type
    // Converting to byte buffer
    final byte[] buffer = this.converter.fromConnectData("dummy",
        inputFieldStruct.schema(), inputFieldObject);
    // Encode buffer in UTF_8 string
    String inputFieldValue = new String(buffer, Charsets.UTF_8);
    // Append the value in outputStruct
    outputStruct.put(this.config.inputFieldName, inputFieldValue);

    return new SchemaAndValue(outputSchema, outputStruct);
  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    return schemaAndValue(inputSchema, input);
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
