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
final class XmlDataConstants
{

   private XmlDataConstants()
   {
      // Utility
   }
   static final String XML_VERSION = "1.0";
   static final String DOCUMENT_PARENT = "hornetq-journal";
   static final String BINDINGS_PARENT = "bindings";
   static final String BINDINGS_CHILD = "binding";
   static final String BINDING_ADDRESS = "address";
   static final String BINDING_FILTER_STRING = "filter-string";
   static final String BINDING_QUEUE_NAME = "queue-name";
   static final String BINDING_ID = "id";
   static final String MESSAGES_PARENT = "messages";
   static final String MESSAGES_CHILD = "message";
   static final String MESSAGE_ID = "id";
   static final String MESSAGE_PRIORITY = "priority";
   static final String MESSAGE_EXPIRATION = "expiration";
   static final String MESSAGE_TIMESTAMP = "timestamp";
   static final String DEFAULT_TYPE_PRETTY = "default";
   static final String BYTES_TYPE_PRETTY = "bytes";
   static final String MAP_TYPE_PRETTY = "map";
   static final String OBJECT_TYPE_PRETTY = "object";
   static final String STREAM_TYPE_PRETTY = "stream";
   static final String TEXT_TYPE_PRETTY = "text";
   static final String MESSAGE_TYPE = "type";
   static final String MESSAGE_IS_LARGE = "isLarge";
   static final String MESSAGE_USER_ID = "user-id";
   static final String MESSAGE_BODY = "body";
   static final String PROPERTIES_PARENT = "properties";
   static final String PROPERTIES_CHILD = "property";
   static final String PROPERTY_NAME = "name";
   static final String PROPERTY_VALUE = "value";
   static final String PROPERTY_TYPE = "type";
   static final String QUEUES_PARENT = "queues";
   static final String QUEUES_CHILD = "queue";
   static final String QUEUE_NAME = "name";
   static final String PROPERTY_TYPE_BOOLEAN = "boolean";
   static final String PROPERTY_TYPE_BYTE = "byte";
   static final String PROPERTY_TYPE_BYTES = "bytes";
   static final String PROPERTY_TYPE_SHORT = "short";
   static final String PROPERTY_TYPE_INTEGER = "integer";
   static final String PROPERTY_TYPE_LONG = "long";
   static final String PROPERTY_TYPE_FLOAT = "float";
   static final String PROPERTY_TYPE_DOUBLE = "double";
   static final String PROPERTY_TYPE_STRING = "string";
   static final String PROPERTY_TYPE_SIMPLE_STRING = "simple-string";
}