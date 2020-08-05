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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class FieldToJSONStringConfig extends AbstractConfig {
    public final String inputFieldName;
    public final String outputFieldName;

    public FieldToJSONStringConfig(Map<String, ?> settings) {
        super(config(), settings);
        this.inputFieldName = getString(INPUT_FIELD_NAME_CONF);
        this.outputFieldName = getString(OUTPUT_FIELD_NAME_CONF);
    }

    public static final String INPUT_FIELD_NAME_CONF = "input.field.name";
    static final String INPUT_FIELD_NAME_DOC = "The field on the struct to be JSON stringify. ";
    public static final String OUTPUT_FIELD_NAME_CONF = "output.field.name";
    static final String OUTPUT_FIELD_NAME_DOC = "The field to place the JSON stringify value into.";

    public static ConfigDef config() {
        return new ConfigDef()
                .define(INPUT_FIELD_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_FIELD_NAME_DOC)
                .define(OUTPUT_FIELD_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, OUTPUT_FIELD_NAME_DOC);
    }

}
