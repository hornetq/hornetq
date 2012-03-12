/*
 * Copyright 2010 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.persistence.impl.journal;

/**
 * The constants shared by <code>org.hornetq.core.persistence.impl.journal.XmlDataImporter</code> and
 * <code>org.hornetq.core.persistence.impl.journal.XmlDataExporter</code>.
 *
 * @author Justin Bertram
 */
public class XmlDataConstants
{
   public static final String XML_VERSION = "1.0";
   public static final String DOCUMENT_PARENT = "hornetq-journal";
   public static final String BINDINGS_PARENT = "bindings";
   public static final String BINDINGS_CHILD = "binding";
   public static final String BINDING_ADDRESS = "address";
   public static final String BINDING_FILTER_STRING = "filter-string";
   public static final String BINDING_QUEUE_NAME = "queue-name";
   public static final String BINDING_ID = "id";
   public static final String MESSAGES_PARENT = "messages";
   public static final String MESSAGES_CHILD = "message";
   public static final String MESSAGE_ID = "id";
   public static final String MESSAGE_PRIORITY = "priority";
   public static final String MESSAGE_EXPIRATION = "expiration";
   public static final String MESSAGE_TIMESTAMP = "timestamp";
   public static final String DEFAULT_TYPE_PRETTY = "default";
   public static final String BYTES_TYPE_PRETTY = "bytes";
   public static final String MAP_TYPE_PRETTY = "map";
   public static final String OBJECT_TYPE_PRETTY = "object";
   public static final String STREAM_TYPE_PRETTY = "stream";
   public static final String TEXT_TYPE_PRETTY = "text";
   public static final String MESSAGE_TYPE = "type";
   public static final String MESSAGE_IS_LARGE = "isLarge";
   public static final String MESSAGE_USER_ID = "user-id";
   public static final String MESSAGE_BODY = "body";
   public static final String PROPERTIES_PARENT = "properties";
   public static final String PROPERTIES_CHILD = "property";
   public static final String PROPERTY_NAME = "name";
   public static final String PROPERTY_VALUE = "value";
   public static final String PROPERTY_TYPE = "type";
   public static final String QUEUES_PARENT = "queues";
   public static final String QUEUES_CHILD = "queue";
   public static final String QUEUE_NAME = "name";
   public static final String PROPERTY_TYPE_BOOLEAN = "boolean";
   public static final String PROPERTY_TYPE_BYTE = "byte";
   public static final String PROPERTY_TYPE_BYTES = "bytes";
   public static final String PROPERTY_TYPE_SHORT = "short";
   public static final String PROPERTY_TYPE_INTEGER = "integer";
   public static final String PROPERTY_TYPE_LONG = "long";
   public static final String PROPERTY_TYPE_FLOAT = "float";
   public static final String PROPERTY_TYPE_DOUBLE = "double";
   public static final String PROPERTY_TYPE_STRING = "string";
   public static final String PROPERTY_TYPE_SIMPLE_STRING = "simple-string";
}