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

package org.hornetq.core.message.impl;

import static org.hornetq.utils.DataConstants.SIZE_BOOLEAN;
import static org.hornetq.utils.DataConstants.SIZE_BYTE;
import static org.hornetq.utils.DataConstants.SIZE_INT;
import static org.hornetq.utils.DataConstants.SIZE_LONG;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.Message;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.message.LargeMessageEncodingContext;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * A concrete implementation of a message
 *
 * All messages handled by HornetQ core are of this type
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 2740 $</tt>
 *
 *
 * $Id: MessageSupport.java 2740 2007-05-30 11:36:28Z timfox $
 */
public abstract class MessageImpl implements Message
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageImpl.class);

   public static final SimpleString HDR_ACTUAL_EXPIRY_TIME = new SimpleString("_HQ_ACTUAL_EXPIRY");

   public static final SimpleString HDR_ORIGINAL_DESTINATION = new SimpleString("_HQ_ORIG_DESTINATION");

   public static final SimpleString HDR_ORIG_MESSAGE_ID = new SimpleString("_HQ_ORIG_MESSAGE_ID");

   public static final SimpleString HDR_GROUP_ID = new SimpleString("_HQ_GROUP_ID");

   public static final SimpleString HDR_SCHEDULED_DELIVERY_TIME = new SimpleString("_HQ_SCHED_DELIVERY");
   
   public static final SimpleString HDR_DUPLICATE_DETECTION_ID = new SimpleString("_HQ_DUPL_ID");

   public static final SimpleString HDR_ROUTE_TO_IDS = new SimpleString("_HQ_ROUTE_TO");
   
   public static final SimpleString HDR_FROM_CLUSTER = new SimpleString("_HQ_FROM_CLUSTER");

   public static final SimpleString HDR_LAST_VALUE_NAME = new SimpleString("_HQ_LVQ_NAME");
  
   // Attributes ----------------------------------------------------

   protected long messageID;

   private SimpleString destination;

   protected byte type;

   protected boolean durable;

   /** GMT milliseconds at which this message expires. 0 means never expires * */
   private long expiration;

   private long timestamp;

   private TypedProperties properties;

   private byte priority;

   private HornetQBuffer body;

   /** Used on LargeMessages */
   private InputStream bodyInputStream;
   
   // Constructors --------------------------------------------------

   protected MessageImpl()
   {
      properties = new TypedProperties();
   }

   /**
    * overridden by the client message, we need access to the connection so we can create the appropriate HornetQBuffer.
    * @param type
    * @param durable
    * @param expiration
    * @param timestamp
    * @param priority
    * @param body
    */
   protected MessageImpl(final byte type,
                         final boolean durable,
                         final long expiration,
                         final long timestamp,
                         final byte priority,
                         final HornetQBuffer body)
   {
      this();
      this.type = type;
      this.durable = durable;
      this.expiration = expiration;
      this.timestamp = timestamp;
      this.priority = priority;
      this.body = body;
   }

   /*
    * Copy constructor
    */
   protected MessageImpl(final MessageImpl other)
   {
      this();
      this.messageID = other.messageID;
      this.destination = other.destination;
      this.type = other.type;
      this.durable = other.durable;
      this.expiration = other.expiration;
      this.timestamp = other.timestamp;
      this.priority = other.priority;
      this.properties = new TypedProperties(other.properties);
      this.body = other.body;
   }
   
   /*
    * Copy constructor
    */
   protected MessageImpl(final Message other)
   {
      this();
      this.messageID = other.getMessageID();
      this.destination = other.getDestination();
      this.type = other.getType();
      this.durable = other.isDurable();
      this.expiration = other.getExpiration();
      this.timestamp = other.getTimestamp();
      this.priority = other.getPriority();
      this.properties = new TypedProperties(other.getProperties());
      this.body = other.getBody();
   }
   

   protected MessageImpl(final long messageID)
   {
      this();
      this.messageID = messageID;
   }

   // Message implementation ----------------------------------------

   public void encode(final HornetQBuffer buffer)
   {
      encodeHeadersAndProperties(buffer);
      buffer.writeInt(getBodySize());
      encodeBody(buffer);
   }
   
   public int getEncodeSize()
   {
      return getHeadersAndPropertiesEncodeSize() + SIZE_INT + getBodySize();
   }

   public int getHeadersAndPropertiesEncodeSize()
   {
      return SIZE_LONG + /* Destination */SimpleString.sizeofString(destination) +
      /* Type */SIZE_BYTE +
      /* Durable */SIZE_BOOLEAN +
      /* Expiration */SIZE_LONG +
      /* Timestamp */SIZE_LONG +
      /* Priority */SIZE_BYTE +
      /* PropertySize and Properties */properties.getEncodeSize();
   }

   public int getBodySize()
   {
      return body.writerIndex();
   }

   public void encodeHeadersAndProperties(HornetQBuffer buffer)
   {
      buffer.writeLong(messageID);
      buffer.writeSimpleString(destination);
      buffer.writeByte(type);
      buffer.writeBoolean(durable);
      buffer.writeLong(expiration);
      buffer.writeLong(timestamp);
      buffer.writeByte(priority);
      properties.encode(buffer);
   }

   public void encodeBody(HornetQBuffer buffer)
   {
      HornetQBuffer localBody = getBody();
      buffer.writeBytes(localBody.array(), 0, localBody.writerIndex());
   }

   // Used on Message chunk side
   public void encodeBody(final HornetQBuffer bufferOut, LargeMessageEncodingContext context, int size)
   {
      context.write(bufferOut, size);
   }

   public void decode(final HornetQBuffer buffer)
   {
      decodeHeadersAndProperties(buffer);

      decodeBody(buffer);
   }

   public void decodeHeadersAndProperties(final HornetQBuffer buffer)
   {
      messageID = buffer.readLong();
      destination = buffer.readSimpleString();
      type = buffer.readByte();
      durable = buffer.readBoolean();
      expiration = buffer.readLong();
      timestamp = buffer.readLong();
      priority = buffer.readByte();
      properties.decode(buffer);
   }

   public void decodeBody(final HornetQBuffer buffer)
   {
      int len = buffer.readInt();
      byte[] bytes = new byte[len];
      buffer.readBytes(bytes);

      // Reuse the same body on the initial body created
      body = ChannelBuffers.dynamicBuffer(bytes); 
   }

   public long getMessageID()
   {
      return messageID;
   }

   public SimpleString getDestination()
   {
      return destination;
   }

   public void setDestination(final SimpleString destination)
   {
      this.destination = destination;
   }

   public byte getType()
   {
      return type;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public void setDurable(final boolean durable)
   {
      this.durable = durable;
   }

   public long getExpiration()
   {
      return expiration;
   }

   public void setExpiration(final long expiration)
   {
      this.expiration = expiration;
   }

   public long getTimestamp()
   {
      return timestamp;
   }

   public void setTimestamp(final long timestamp)
   {
      this.timestamp = timestamp;
   }

   public byte getPriority()
   {
      return priority;
   }

   public void setPriority(final byte priority)
   {
      this.priority = priority;
   }

   public boolean isExpired()
   {
      if (expiration == 0)
      {
         return false;
      }

      return System.currentTimeMillis() - expiration >= 0;
   }
   
   /**
    * @return the bodyInputStream
    */
   public InputStream getBodyInputStream()
   {
      return bodyInputStream;
   }

   /**
    * @param bodyInputStream the bodyInputStream to set
    */
   public void setBodyInputStream(InputStream bodyInputStream)
   {
      this.bodyInputStream = bodyInputStream;
   }

   public Map<String, Object> toMap()
   {
      Map<String, Object> map = new HashMap<String, Object>();
      
      map.put("messageID", messageID);
      map.put("destination", destination.toString());
      map.put("type", type);
      map.put("durable", durable);
      map.put("expiration", expiration);
      map.put("timestamp", timestamp);
      map.put("priority", priority);
      for (SimpleString propName: properties.getPropertyNames())
      {
         map.put(propName.toString(), properties.getProperty(propName));
      }
      return map;      
   }
     
   
   // Properties
   // ---------------------------------------------------------------------------------------

   public void putBooleanProperty(final SimpleString key, final boolean value)
   {
      properties.putBooleanProperty(key, value);
   }

   public void putByteProperty(final SimpleString key, final byte value)
   {
      properties.putByteProperty(key, value);
   }

   public void putBytesProperty(final SimpleString key, final byte[] value)
   {
      properties.putBytesProperty(key, value);
   }

   public void putShortProperty(final SimpleString key, final short value)
   {
      properties.putShortProperty(key, value);
   }

   public void putIntProperty(final SimpleString key, final int value)
   {
      properties.putIntProperty(key, value);
   }

   public void putLongProperty(final SimpleString key, final long value)
   {
      properties.putLongProperty(key, value);
   }

   public void putFloatProperty(final SimpleString key, final float value)
   {
      properties.putFloatProperty(key, value);
   }

   public void putDoubleProperty(final SimpleString key, final double value)
   {
      properties.putDoubleProperty(key, value);
   }

   public void putStringProperty(final SimpleString key, final SimpleString value)
   {
      properties.putStringProperty(key, value);
   }
   
   public void putBooleanProperty(final String key, final boolean value)
   {
      properties.putBooleanProperty(new SimpleString(key), value);
   }

   public void putByteProperty(final String key, final byte value)
   {
      properties.putByteProperty(new SimpleString(key), value);
   }

   public void putBytesProperty(final String key, final byte[] value)
   {
      properties.putBytesProperty(new SimpleString(key), value);
   }

   public void putShortProperty(final String key, final short value)
   {
      properties.putShortProperty(new SimpleString(key), value);
   }

   public void putIntProperty(final String key, final int value)
   {
      properties.putIntProperty(new SimpleString(key), value);
   }

   public void putLongProperty(final String key, final long value)
   {
      properties.putLongProperty(new SimpleString(key), value);
   }

   public void putFloatProperty(final String key, final float value)
   {
      properties.putFloatProperty(new SimpleString(key), value);
   }

   public void putDoubleProperty(final String key, final double value)
   {
      properties.putDoubleProperty(new SimpleString(key), value);
   }

   public void putStringProperty(final String key, final String value)
   {
      properties.putStringProperty(new SimpleString(key), new SimpleString(value));
   }

   public void putTypedProperties(TypedProperties otherProps)
   {
      properties.putTypedProperties(otherProps);
   }

   public Object getProperty(final SimpleString key)
   {
      return properties.getProperty(key);
   }
   
   public Object getProperty(final String key)
   {
      return properties.getProperty(new SimpleString(key));
   }

   public Object removeProperty(final SimpleString key)
   {
      return properties.removeProperty(key);
   }
   
   public Object removeProperty(final String key)
   {
      return properties.removeProperty(new SimpleString(key));
   }

   public boolean containsProperty(final SimpleString key)
   {
      return properties.containsProperty(key);
   }
   
   public boolean containsProperty(final String key)
   {
      return properties.containsProperty(new SimpleString(key));
   }

   public Set<SimpleString> getPropertyNames()
   {
      return properties.getPropertyNames();
   }

   public TypedProperties getProperties()
   {
      return this.properties;
   }
      
   // Body
   // -------------------------------------------------------------------------------------

   public HornetQBuffer getBody()
   {
      return body;
   }

   public void setBody(final HornetQBuffer body)
   {
      this.body = body;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
