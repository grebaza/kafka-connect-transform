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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public class FieldToJSONStringConfig extends AbstractConfig {
  private static final String WHOLE_VALUE_CAST = null;
  private enum FieldType {
    OUTPUT
  }
  private static final Set<Schema.Type> SUPPORTED_CAST_OUTPUT_TYPES = EnumSet.of(
    Schema.Type.STRING, Schema.Type.BYTES
  );

  public final String inputFieldName;
  public final boolean schemasEnable;
  //public final Schema.Type outputSchemaType;

  public static final String SPEC_CONFIG = "spec";
  public static final String SPEC_DOC = "The field on the struct to be JSON" +
    " stringify. ";
  public static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
  public static final String SCHEMAS_ENABLE_DOC = "Flag to determine if the" +
    " JSON data should include the schema.";

  public FieldToJSONStringConfig(Map<String, ?> settings) {
    super(config(), settings);
    this.inputFieldName = getString(INPUT_FIELD_NAME_CONFIG);
    this.schemasEnable = getBoolean(SCHEMAS_ENABLE_CONFIG);
    //this.outputSchemaType = ConfigUtils.getEnum(
    //  Schema.Type.class, this, OUTPUT_SCHEMA_CONFIG
    //);
  }


  public static ConfigDef config() {

    return new ConfigDef()
      .define(SPEC_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
        new ConfigDef.Validator() {
          @SuppressWarnings("unchecked")
          @Override
          public void ensureValid(String name, Object valueObject) {
            List<String> value = (List<String>) valueObject;
            if (value == null || value.isEmpty()) {
              throw new ConfigException("Must specify at least one field to JSON cast.");
            }
            parseFieldTypes(value);
          }

          @Override
          public String toString() {
            return "list of colon-delimited pairs, e.g. " +
              "<code>foo:bar:baz,abc:rst:xyz</code>";
          }
        },
        ConfigDef.Importance.HIGH,
          "List of fields,schema type, and new fields' names " +
          "field1:schema-type:new-field1,field1:schema-type:new-field1."
      )
      .define(
        SCHEMAS_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.MEDIUM, SCHEMAS_ENABLE_DOC
      );
  }

  private static Map<String, Schema.Type> parseFieldTypes(List<String> mappings) {
    final Map<String, Schema.Type> m = new HashMap<>();
    boolean isWholeValueCast = false;
    for (String mapping : mappings) {
      final String[] parts = mapping.split(":");
      if (parts.length > 3) {
        throw new ConfigException(ReplaceField.ConfigName.RENAME, mappings,
          "Invalid rename mapping: " + mapping);
      }
      // Duda
      if (parts.length == 1) {
        Schema.Type targetType = Schema.Type.valueOf(parts[0].trim().toUpperCase(Locale.ROOT));
        m.put(WHOLE_VALUE_CAST, validCastType(targetType, FieldType.OUTPUT));
        isWholeValueCast = true;
      // Fin duda
      } else {
        Schema.Type type;
        try {
          type = Schema.Type.valueOf(parts[1].trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
          throw new ConfigException("Invalid type found in casting spec: " + parts[1].trim(), e);
        }
        m.put(parts[0].trim(), validCastType(type, FieldType.OUTPUT));
      }
    }
    if (isWholeValueCast && mappings.size() > 1) {
      throw new ConfigException("Cast transformations that specify a type to cast the entire value to "
        + "may ony specify a single cast in their spec");
    }
    return m;
  }

  private static Schema.Type validCastType(Schema.Type type, FieldType fieldType) {
    switch (fieldType) {
      case OUTPUT:
        if (!SUPPORTED_CAST_OUTPUT_TYPES.contains(type)) {
          throw new ConfigException("Cast transformation does not support casting to " +
            type + "; supported types are " + SUPPORTED_CAST_OUTPUT_TYPES);
        }
        break;
    }
    return type;
  }

}
