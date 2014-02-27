/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2010, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.hornetq.integration.vertx;

import java.util.HashSet;
import java.util.Set;

public class VertxConstants
{
   // org.vertx.java.core.eventbus.impl.MessageFactory
   public static final int TYPE_PING = 0;
   public static final int TYPE_BUFFER = 1;
   public static final int TYPE_BOOLEAN = 2;
   public static final int TYPE_BYTEARRAY = 3;
   public static final int TYPE_BYTE = 4;
   public static final int TYPE_CHARACTER = 5;
   public static final int TYPE_DOUBLE = 6;
   public static final int TYPE_FLOAT = 7;
   public static final int TYPE_INT = 8;
   public static final int TYPE_LONG = 9;
   public static final int TYPE_SHORT = 10;
   public static final int TYPE_STRING = 11;
   public static final int TYPE_JSON_OBJECT = 12;
   public static final int TYPE_JSON_ARRAY = 13;
   public static final int TYPE_REPLY_FAILURE = 100;
   public static final int TYPE_RAWBYTES = 200;


   public static final String PORT = "port";
   public static final String HOST = "host";
   public static final String QUEUE_NAME = "queue";
   public static final String VERTX_ADDRESS = "vertx-address";
   public static final String VERTX_PUBLISH = "publish";
   public static final String VERTX_QUORUM_SIZE = "quorum-size";
   public static final String VERTX_HA_GROUP = "ha-group";

   public static final Set<String> ALLOWABLE_INCOMING_CONNECTOR_KEYS;
   public static final Set<String> REQUIRED_INCOMING_CONNECTOR_KEYS;
   public static final Set<String> ALLOWABLE_OUTGOING_CONNECTOR_KEYS;
   public static final Set<String> REQUIRED_OUTGOING_CONNECTOR_KEYS;
   public static final int INITIAL_MESSAGE_BUFFER_SIZE = 50;
   public static final String VERTX_MESSAGE_REPLYADDRESS = "vertx.message.replyaddress";
   public static final String VERTX_MESSAGE_TYPE = "vertx.message.type";

   static
   {
      ALLOWABLE_INCOMING_CONNECTOR_KEYS = new HashSet<String>();
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(PORT);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(HOST);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(QUEUE_NAME);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(VERTX_ADDRESS);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(VERTX_QUORUM_SIZE);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(VERTX_HA_GROUP);

      REQUIRED_INCOMING_CONNECTOR_KEYS = new HashSet<String>();
      REQUIRED_INCOMING_CONNECTOR_KEYS.add(QUEUE_NAME);

      ALLOWABLE_OUTGOING_CONNECTOR_KEYS = new HashSet<String>();
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(PORT);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(HOST);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(QUEUE_NAME);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(VERTX_ADDRESS);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(VERTX_PUBLISH);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(VERTX_QUORUM_SIZE);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(VERTX_HA_GROUP);

      REQUIRED_OUTGOING_CONNECTOR_KEYS = new HashSet<String>();
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(QUEUE_NAME);
   }
}
