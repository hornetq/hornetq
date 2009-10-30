/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.message;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * A message is a routable instance that has a payload.
 * 
 * The payload is opaque to the messaging system.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">ClebertSuconic</a>
 * @version <tt>$Revision: 3341 $</tt>
 *
 * $Id: Message.java 3341 2007-11-19 14:34:57Z timfox $
 */
public interface Message
{   
   long getMessageID();
   
   SimpleString getDestination();
   
   void setDestination(SimpleString destination);
   
   byte getType();   
      
   boolean isDurable();
   
   void setDurable(boolean durable);
   
   long getExpiration();

   boolean isExpired();
   
   void setExpiration(long expiration);
   
   long getTimestamp();
   
   void setTimestamp(long timestamp);
   
   byte getPriority();
   
   void setPriority(byte priority);
   
   int getEncodeSize();

   void encode(HornetQBuffer buffer);
   
   void decode(HornetQBuffer buffer);
   
   
   int getHeadersAndPropertiesEncodeSize();
   
   void encodeHeadersAndProperties(HornetQBuffer buffer);
   
   void decodeHeadersAndProperties(HornetQBuffer buffer);
      
   int getBodySize();

   boolean isLargeMessage();
   
   long getLargeBodySize();
         
   // Used on Message chunk
   void encodeBody(HornetQBuffer buffer, long start, int size);
   
   /** Set the InputStream used on a message that will be sent over a producer */
   void setBodyInputStream(InputStream stream);
   
   /** Get the InputStream used on a message that will be sent over a producer */
   InputStream getBodyInputStream();
   
   
   void encodeBody(HornetQBuffer buffer);
   
   void decodeBody(HornetQBuffer buffer);
      
   // Properties
   // ------------------------------------------------------------------
   
   TypedProperties getProperties();
   
   void putBooleanProperty(SimpleString key, boolean value);
   
   void putByteProperty(SimpleString key, byte value);
   
   void putBytesProperty(SimpleString key, byte[] value);
   
   void putShortProperty(SimpleString key, short value);
   
   void putIntProperty(SimpleString key, int value);
   
   void putLongProperty(SimpleString key, long value);
   
   void putFloatProperty(SimpleString key, float value);
   
   void putDoubleProperty(SimpleString key, double value);
   
   void putStringProperty(SimpleString key, SimpleString value);
   
   void putBooleanProperty(String key, boolean value);
   
   void putByteProperty(String key, byte value);
   
   void putBytesProperty(String key, byte[] value);
   
   void putShortProperty(String key, short value);
   
   void putIntProperty(String key, int value);
   
   void putLongProperty(String key, long value);
   
   void putFloatProperty(String key, float value);
   
   void putDoubleProperty(String key, double value);
   
   void putStringProperty(String key, String value);
   
   void putTypedProperties(TypedProperties properties);

   // TODO - should have typed property getters and do conversions herein
   
   Object getProperty(SimpleString key);
   
   Object removeProperty(SimpleString key);
   
   boolean containsProperty(SimpleString key);
   
   Object getProperty(String key);
   
   Object removeProperty(String key);
   
   boolean containsProperty(String key);
   
   Set<SimpleString> getPropertyNames();
   
   Map<String, Object> toMap();
   
   // Body
   // ---------------------------------------------------------------------------------
   
   HornetQBuffer getBody();
   
   void setBody(HornetQBuffer body);
   
}
