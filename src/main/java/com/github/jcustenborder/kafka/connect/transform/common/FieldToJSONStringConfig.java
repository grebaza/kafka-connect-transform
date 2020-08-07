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
  public final String inputFieldName;
  public final String outputFieldName;
  public final boolean schemasEnable;
  public final Schema.Type outputSchemaType;

  public static final String SPEC_CONFIG = "input.field.name";
  public static final String SPEC_DOC = "The field on the struct to be JSON" +
    " stringify. ";
  public static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
  public static final String SCHEMAS_ENABLE_DOC = "Flag to determine if the" +
    " JSON data should include the schema.";

  public FieldToJSONStringConfig(Map<String, ?> settings) {
    super(config(), settings);
    this.inputFieldName = getString(INPUT_FIELD_NAME_CONFIG);
    this.outputFieldName = getString(OUTPUT_FIELD_NAME_CONFIG);
    this.schemasEnable = getBoolean(SCHEMAS_ENABLE_CONFIG);
    this.outputSchemaType = ConfigUtils.getEnum(
        Schema.Type.class, this, OUTPUT_SCHEMA_CONFIG
        );
  }


  public static ConfigDef config() {

    return new ConfigDef()
      .define(
        SCHEMAS_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, false,
        ConfigDef.Importance.MEDIUM, SCHEMAS_ENABLE_DOC
       )
    .define(SPEC_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
        new ConfigDef.Validator() {
          @SuppressWarnings("unchecked")
          @Override
          public void ensureValid(String name, Object valueObject) {
            List<String> value = (List<String>) valueObject;
            if (value == null || value.isEmpty()) {
              throw new ConfigException("Must specify at least one field to cast.");
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
        );
  }

}
