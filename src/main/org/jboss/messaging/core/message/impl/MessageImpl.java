/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

package org.jboss.messaging.core.message.impl;

import static org.jboss.messaging.util.DataConstants.SIZE_BOOLEAN;
import static org.jboss.messaging.util.DataConstants.SIZE_BYTE;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;
import static org.jboss.messaging.util.DataConstants.SIZE_LONG;

import java.util.Set;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TypedProperties;

/**
 * A concrete implementation of a message
 *
 * All messages handled by JBM core are of this type
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

   public static final SimpleString HDR_ACTUAL_EXPIRY_TIME = new SimpleString("JBM_ACTUAL_EXPIRY");

   public static final SimpleString HDR_ORIGINAL_DESTINATION = new SimpleString("JBM_ORIG_DESTINATION");

   public static final SimpleString HDR_ORIG_MESSAGE_ID = new SimpleString("JBM_ORIG_MESSAGE_ID");

   public static final SimpleString HDR_GROUP_ID = new SimpleString("JBM_GROUP_ID");

   public static final SimpleString HDR_SCHEDULED_DELIVERY_TIME = new SimpleString("JBM_SCHED_DELIVERY");
   
   public static final SimpleString HDR_DUPLICATE_DETECTION_ID = new SimpleString("JBM_DUPL_ID");
   
   public static final SimpleString HDR_ROUTE_TO_PREFIX = new SimpleString("JBM_ROUTE_TO:");
      
   public static final SimpleString HDR_RESET_QUEUE_DATA = new SimpleString("JBM_RESET_QUEUE_DATA");
      
   public static final SimpleString HDR_FROM_CLUSTER = new SimpleString("JBM_FROM_CLUSTER");
   
   // Attributes ----------------------------------------------------

   protected long messageID;

   private SimpleString destination;

   private byte type;

   protected boolean durable;

   /** GMT milliseconds at which this message expires. 0 means never expires * */
   private long expiration;

   private long timestamp;

   private TypedProperties properties;

   private byte priority;

   private MessagingBuffer body;
   
   // Constructors --------------------------------------------------

   protected MessageImpl()
   {
      properties = new TypedProperties();
   }

   /**
    * overridden by the client message, we need access to the connection so we can create the appropriate MessagingBuffer.
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
                         final MessagingBuffer body)
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
   

   protected MessageImpl(final long messageID)
   {
      this();
      this.messageID = messageID;
   }

   // Message implementation ----------------------------------------

   public void encode(final MessagingBuffer buffer)
   {
      encodeProperties(buffer);
      buffer.putInt(getBodySize());
      encodeBody(buffer);
   }
   
   public int getEncodeSize()
   {
      return getPropertiesEncodeSize() + SIZE_INT + getBodySize();
   }

   public int getPropertiesEncodeSize()
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
      return /* BodySize and Body */body.limit();
   }

   public void encodeProperties(MessagingBuffer buffer)
   {
      buffer.putLong(messageID);
      buffer.putSimpleString(destination);
      buffer.putByte(type);
      buffer.putBoolean(durable);
      buffer.putLong(expiration);
      buffer.putLong(timestamp);
      buffer.putByte(priority);
      properties.encode(buffer);
   }

   public void encodeBody(MessagingBuffer buffer)
   {
      MessagingBuffer localBody = getBody();
      buffer.putBytes(localBody.array(), 0, localBody.limit());
   }

   // Used on Message chunk
   public void encodeBody(MessagingBuffer buffer, long start, int size)
   {
      buffer.putBytes(body.array(), (int)start, size);
   }

   public void decode(final MessagingBuffer buffer)
   {
      decodeProperties(buffer);

      decodeBody(buffer);
   }

   public void decodeProperties(final MessagingBuffer buffer)
   {
      messageID = buffer.getLong();
      destination = buffer.getSimpleString();
      type = buffer.getByte();
      durable = buffer.getBoolean();
      expiration = buffer.getLong();
      timestamp = buffer.getLong();
      priority = buffer.getByte();
      properties.decode(buffer);
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      int len = buffer.getInt();
      // TODO - this can be optimised
      byte[] bytes = new byte[len];
      buffer.getBytes(bytes);
      // body = new ByteBufferWrapper(ByteBuffer.wrap(bytes));
      // body.position(body.limit());
      body = buffer.createNewBuffer(len);
      body.putBytes(bytes);

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

   public void putTypedProperties(TypedProperties otherProps)
   {
      properties.putTypedProperties(otherProps);
   }

   public Object getProperty(final SimpleString key)
   {
      return properties.getProperty(key);
   }

   public Object removeProperty(final SimpleString key)
   {
      return properties.removeProperty(key);
   }

   public boolean containsProperty(final SimpleString key)
   {
      return properties.containsProperty(key);
   }

   public Set<SimpleString> getPropertyNames()
   {
      return properties.getPropertyNames();
   }

   // Body
   // -------------------------------------------------------------------------------------

   public MessagingBuffer getBody()
   {
      return body;
   }

   public void setBody(final MessagingBuffer body)
   {
      this.body = body;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected TypedProperties getProperties()
   {
      return this.properties;
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
