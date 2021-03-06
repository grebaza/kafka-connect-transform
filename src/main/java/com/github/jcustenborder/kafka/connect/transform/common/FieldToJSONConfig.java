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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;

public class FieldToJSONConfig extends AbstractConfig {
  private enum FieldType {
    OUTPUT
  }
  public static final class FieldSettings {
    public String outputName;
    public Schema.Type outputSchema;
  }
  private static final Set<Schema.Type> SUPPORTED_CAST_OUTPUT_TYPES =
      EnumSet.of(Schema.Type.STRING, Schema.Type.BYTES);

  public final boolean schemasEnable;
  public Map<String, FieldSettings> conversions;

  public static final String SPEC_CONFIG = "spec";
  public static final String SPEC_DOC =
      "List of triads describing input and output fields. " +
      "Input fields are JSON stringified. " +
      "Output fields Schema can be 'string' or 'bytes'.";
  public static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
  public static final String SCHEMAS_ENABLE_DOC =
      "Flag to determine if the JSON data should include the schema.";

  public FieldToJSONConfig(Map<String, ?> settings) {
    super(config(), settings);
    this.schemasEnable = getBoolean(SCHEMAS_ENABLE_CONFIG);
    this.conversions = parseSpecs(getList(SPEC_CONFIG));
  }

  public static ConfigDef config() {
    return new ConfigDef()
      .define(
        SPEC_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
        new ConfigDef.Validator() {
          @SuppressWarnings("unchecked")
          @Override
          public void ensureValid(String name, Object valueObject) {
            List<String> value = (List<String>) valueObject;
            if (value == null || value.isEmpty()) {
              throw new ConfigException(
                  "Must specify at least one field for FieldToJSON.");
            }
            parseSpecs(value);
          }

          @Override
          public String toString() {
            return "list of colon-delimited triads, e.g. " +
              "<code>foo:bar:baz,abc:rst:xyz</code>";
          }
        },
        ConfigDef.Importance.HIGH,
          "List of triads (input field and new field schema, and name) " +
          "input_field:new_field_schema:new_field_name."
      )
      .define(
        SCHEMAS_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.MEDIUM, SCHEMAS_ENABLE_DOC
      );
  }

  private static Map<String, FieldSettings> parseSpecs(List<String> triads) {
    final Map<String, FieldSettings> mo = new HashMap<>();
    for (String triad : triads) {
      final FieldSettings fieldSettings = new FieldSettings();
      final String[] parts = triad.split(":");
      if (parts.length != 3) {
        throw new ConfigException(
            "FieldToJSON", triads,
            "Invalid spec config for triad: " + triad);
      } else {
        Schema.Type type;
        try {
          type = Schema.Type.valueOf(
              parts[1].trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
          throw new ConfigException(
              String.format(
                  "Config (%s): schema '%s' for field '%s' is not supported.",
                  FieldToJSONConfig.SPEC_CONFIG,
                  parts[1].trim(),
                  parts[0].trim()));
        }
        fieldSettings.outputSchema = validCastType(type, FieldType.OUTPUT);
        fieldSettings.outputName = parts[2].trim();
        mo.put(parts[0].trim(), fieldSettings);
      }
    }
    return mo;
  }

  private static Schema.Type validCastType(Schema.Type type, FieldType fieldType) {
    switch (fieldType) {
      case OUTPUT:
        if (!SUPPORTED_CAST_OUTPUT_TYPES.contains(type)) {
          throw new ConfigException(
              "Transformation does not support " + type + " Schema type" +
              "; supported types are " + SUPPORTED_CAST_OUTPUT_TYPES);
        }
        break;
    }
    return type;
  }

}
