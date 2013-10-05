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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQPropertyConversionException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.buffers.impl.ResetLimitWrappedHornetQBuffer;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;

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
public abstract class MessageImpl implements MessageInternal
{
   public static final SimpleString HDR_ROUTE_TO_IDS = new SimpleString("_HQ_ROUTE_TO");

   // used by the bridges to set duplicates
   public static final SimpleString HDR_BRIDGE_DUPLICATE_ID = new SimpleString("_HQ_BRIDGE_DUP");

   public static final int BUFFER_HEADER_SPACE = PacketImpl.PACKET_HEADERS_SIZE;

   public static final int BODY_OFFSET = BUFFER_HEADER_SPACE + DataConstants.SIZE_INT;

   protected long messageID;

   protected SimpleString address;

   protected byte type;

   protected boolean durable;

   /** GMT milliseconds at which this message expires. 0 means never expires * */
   private long expiration;

   protected long timestamp;

   protected TypedProperties properties;

   protected byte priority;

   protected HornetQBuffer buffer;

   protected ResetLimitWrappedHornetQBuffer bodyBuffer;

   protected volatile boolean bufferValid;

   private int endOfBodyPosition = -1;

   private int endOfMessagePosition;

   private boolean copied = true;

   private boolean bufferUsed;

   private UUID userID;

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
    * @param initialMessageBufferSize
    */
   protected MessageImpl(final byte type,
                         final boolean durable,
                         final long expiration,
                         final long timestamp,
                         final byte priority,
                         final int initialMessageBufferSize)
   {
      this();
      this.type = type;
      this.durable = durable;
      this.expiration = expiration;
      this.timestamp = timestamp;
      this.priority = priority;
      createBody(initialMessageBufferSize);
   }

   protected MessageImpl(final int initialMessageBufferSize)
   {
      this();
      createBody(initialMessageBufferSize);
   }

   /*
    * Copy constructor
    */
   protected MessageImpl(final MessageImpl other)
   {
      this(other, other.getProperties());
   }

   /*
    * Copy constructor
    */
   protected MessageImpl(final MessageImpl other, TypedProperties properties)
   {
      messageID = other.getMessageID();
      userID = other.getUserID();
      address = other.getAddress();
      type = other.getType();
      durable = other.isDurable();
      expiration = other.getExpiration();
      timestamp = other.getTimestamp();
      priority = other.getPriority();
      this.properties = new TypedProperties(properties);

      // This MUST be synchronized using the monitor on the other message to prevent it running concurrently
      // with getEncodedBuffer(), otherwise can introduce race condition when delivering concurrently to
      // many subscriptions and bridging to other nodes in a cluster
      synchronized (other)
      {
         bufferValid = other.bufferValid;
         endOfBodyPosition = other.endOfBodyPosition;
         endOfMessagePosition = other.endOfMessagePosition;
         copied = other.copied;

         if (other.buffer != null)
         {
            createBody(other.buffer.capacity());

            // We need to copy the underlying buffer too, since the different messsages thereafter might have different
            // properties set on them, making their encoding different
            buffer = other.buffer.copy(0, other.buffer.capacity());

            buffer.setIndex(other.buffer.readerIndex(), other.buffer.writerIndex());
         }
      }
   }

   // Message implementation ----------------------------------------

   public int getEncodeSize()
   {
      int headersPropsSize = getHeadersAndPropertiesEncodeSize();

      int bodyPos = getEndOfBodyPosition();

      int bodySize = bodyPos - BUFFER_HEADER_SPACE - DataConstants.SIZE_INT;

      return DataConstants.SIZE_INT + bodySize + DataConstants.SIZE_INT + headersPropsSize;
   }

   public int getHeadersAndPropertiesEncodeSize()
   {
      return DataConstants.SIZE_LONG + // Message ID
             DataConstants.SIZE_BYTE + // user id null?
             (userID == null ? 0 : 16) +
             /* address */SimpleString.sizeofNullableString(address) +
             DataConstants./* Type */SIZE_BYTE +
             DataConstants./* Durable */SIZE_BOOLEAN +
             DataConstants./* Expiration */SIZE_LONG +
             DataConstants./* Timestamp */SIZE_LONG +
             DataConstants./* Priority */SIZE_BYTE +
             /* PropertySize and Properties */properties.getEncodeSize();
   }


   public void encodeHeadersAndProperties(final HornetQBuffer buffer)
   {
      buffer.writeLong(messageID);
      buffer.writeNullableSimpleString(address);
      if (userID == null)
      {
         buffer.writeByte(DataConstants.NULL);
      }
      else
      {
         buffer.writeByte(DataConstants.NOT_NULL);
         buffer.writeBytes(userID.asBytes());
      }
      buffer.writeByte(type);
      buffer.writeBoolean(durable);
      buffer.writeLong(expiration);
      buffer.writeLong(timestamp);
      buffer.writeByte(priority);
      properties.encode(buffer);
   }

   public void decodeHeadersAndProperties(final HornetQBuffer buffer)
   {
      messageID = buffer.readLong();
      address = buffer.readNullableSimpleString();
      if (buffer.readByte() == DataConstants.NOT_NULL)
      {
         byte[] bytes = new byte[16];
         buffer.readBytes(bytes);
         userID = new UUID(UUID.TYPE_TIME_BASED, bytes);
      }
      else
      {
         userID = null;
      }
      type = buffer.readByte();
      durable = buffer.readBoolean();
      expiration = buffer.readLong();
      timestamp = buffer.readLong();
      priority = buffer.readByte();
      properties.decode(buffer);
   }

   public void copyHeadersAndProperties(final MessageInternal msg)
   {
      messageID = msg.getMessageID();
      address = msg.getAddress();
      userID = msg.getUserID();
      type = msg.getType();
      durable = msg.isDurable();
      expiration = msg.getExpiration();
      timestamp = msg.getTimestamp();
      priority = msg.getPriority();
      properties = msg.getTypedProperties();
   }

   public HornetQBuffer getBodyBuffer()
   {
      if (bodyBuffer == null)
      {
         bodyBuffer = new ResetLimitWrappedHornetQBuffer(BODY_OFFSET, buffer, this);
      }

      return bodyBuffer;
   }

   public void checkCompletion() throws HornetQException
   {
      // no op on regular messages
   }


   public synchronized HornetQBuffer getBodyBufferCopy()
   {
      // Must copy buffer before sending it

      HornetQBuffer newBuffer = buffer.copy(0, buffer.capacity());

      newBuffer.setIndex(0, getEndOfBodyPosition());

      return new ResetLimitWrappedHornetQBuffer(BODY_OFFSET, newBuffer, null);
   }

   public long getMessageID()
   {
      return messageID;
   }

   public UUID getUserID()
   {
      return userID;
   }

   public void setUserID(final UUID userID)
   {
      this.userID = userID;
   }

   /** this doesn't need to be synchronized as setAddress is protecting the buffer,
    *  not the address*/
   public SimpleString getAddress()
   {
      return address;
   }

   /**
    * The only reason this is synchronized is because of encoding a message versus invalidating the buffer.
    * This synchronization can probably be removed since setAddress is always called from a single thread.
    * However I will keep it as it's harmless and it's been well tested
    */
   public void setAddress(final SimpleString address)
   {
     // This is protecting the buffer
     synchronized (this)
     {
         if (this.address != address)
         {
            this.address = address;

            bufferValid = false;
         }
     }
   }

   public byte getType()
   {
      return type;
   }

   public void setType(byte type)
   {
      this.type = type;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public void setDurable(final boolean durable)
   {
      if (this.durable != durable)
      {
         this.durable = durable;

         bufferValid = false;
      }
   }

   public long getExpiration()
   {
      return expiration;
   }

   public void setExpiration(final long expiration)
   {
      if (this.expiration != expiration)
      {
         this.expiration = expiration;

         bufferValid = false;
      }
   }

   public long getTimestamp()
   {
      return timestamp;
   }

   public void setTimestamp(final long timestamp)
   {
      if (this.timestamp != timestamp)
      {
         this.timestamp = timestamp;

         bufferValid = false;
      }
   }

   public byte getPriority()
   {
      return priority;
   }

   public void setPriority(final byte priority)
   {
      if (this.priority != priority)
      {
         this.priority = priority;

         bufferValid = false;
      }
   }

   public boolean isExpired()
   {
      if (expiration == 0)
      {
         return false;
      }

      return System.currentTimeMillis() - expiration >= 0;
   }

   public Map<String, Object> toMap()
   {
      Map<String, Object> map = new HashMap<String, Object>();

      map.put("messageID", messageID);
      if (userID != null)
      {
         map.put("userID", "ID:" + userID.toString());
      }
      map.put("address", address.toString());
      map.put("type", type);
      map.put("durable", durable);
      map.put("expiration", expiration);
      map.put("timestamp", timestamp);
      map.put("priority", priority);
      for (SimpleString propName : properties.getPropertyNames())
      {
         map.put(propName.toString(), properties.getProperty(propName));
      }
      return map;
   }

   public void decodeFromBuffer(final HornetQBuffer buffer)
   {
      this.buffer = buffer;

      decode();
   }

   public void bodyChanged()
   {
      // If the body is changed we must copy the buffer otherwise can affect the previously sent message
      // which might be in the Netty write queue
      checkCopy();

      bufferValid = false;

      endOfBodyPosition = -1;
   }

   public synchronized void checkCopy()
   {
      if (!copied)
      {
         forceCopy();

         copied = true;
      }
   }

   public synchronized void resetCopied()
   {
      copied = false;
   }

   public int getEndOfMessagePosition()
   {
      return endOfMessagePosition;
   }

   public int getEndOfBodyPosition()
   {
      if (endOfBodyPosition < 0)
      {
         endOfBodyPosition = buffer.writerIndex();
      }
      return endOfBodyPosition;
   }

   // Encode to journal or paging
   public void encode(final HornetQBuffer buff)
   {
      encodeToBuffer();

      buff.writeBytes(buffer, BUFFER_HEADER_SPACE, endOfMessagePosition - BUFFER_HEADER_SPACE);
   }

   // Decode from journal or paging
   public void decode(final HornetQBuffer buff)
   {
      int start = buff.readerIndex();

      endOfBodyPosition = buff.readInt();

      endOfMessagePosition = buff.getInt(endOfBodyPosition - BUFFER_HEADER_SPACE + start);

      int length = endOfMessagePosition - BUFFER_HEADER_SPACE;

      buffer.setIndex(0, BUFFER_HEADER_SPACE);

      buffer.writeBytes(buff, start, length);

      decode();

      buff.readerIndex(start + length);
   }

   public synchronized HornetQBuffer getEncodedBuffer()
   {
      HornetQBuffer buff = encodeToBuffer();

      if (bufferUsed)
      {
         HornetQBuffer copied = buff.copy(0, buff.capacity());

         copied.setIndex(0, endOfMessagePosition);

         return copied;
      }
      else
      {
         buffer.setIndex(0, endOfMessagePosition);

         bufferUsed = true;

         return buffer;
      }
   }

   public void setAddressTransient(final SimpleString address)
   {
      this.address = address;
   }


   // Properties
   // ---------------------------------------------------------------------------------------

   public void putBooleanProperty(final SimpleString key, final boolean value)
   {
      properties.putBooleanProperty(key, value);

      bufferValid = false;
   }

   public void putByteProperty(final SimpleString key, final byte value)
   {
      properties.putByteProperty(key, value);

      bufferValid = false;
   }

   public void putBytesProperty(final SimpleString key, final byte[] value)
   {
      properties.putBytesProperty(key, value);

      bufferValid = false;
   }

   public void putShortProperty(final SimpleString key, final short value)
   {
      properties.putShortProperty(key, value);

      bufferValid = false;
   }

   public void putIntProperty(final SimpleString key, final int value)
   {
      properties.putIntProperty(key, value);

      bufferValid = false;
   }

   public void putLongProperty(final SimpleString key, final long value)
   {
      properties.putLongProperty(key, value);

      bufferValid = false;
   }

   public void putFloatProperty(final SimpleString key, final float value)
   {
      properties.putFloatProperty(key, value);

      bufferValid = false;
   }

   public void putDoubleProperty(final SimpleString key, final double value)
   {
      properties.putDoubleProperty(key, value);

      bufferValid = false;
   }

   public void putStringProperty(final SimpleString key, final SimpleString value)
   {
      properties.putSimpleStringProperty(key, value);

      bufferValid = false;
   }

   public void putObjectProperty(final SimpleString key, final Object value) throws HornetQPropertyConversionException
   {
      TypedProperties.setObjectProperty(key, value, properties);
      bufferValid = false;
   }

   public void putObjectProperty(final String key, final Object value) throws HornetQPropertyConversionException
   {
      putObjectProperty(new SimpleString(key), value);

      bufferValid = false;
   }

   public void putBooleanProperty(final String key, final boolean value)
   {
      properties.putBooleanProperty(new SimpleString(key), value);

      bufferValid = false;
   }

   public void putByteProperty(final String key, final byte value)
   {
      properties.putByteProperty(new SimpleString(key), value);

      bufferValid = false;
   }

   public void putBytesProperty(final String key, final byte[] value)
   {
      properties.putBytesProperty(new SimpleString(key), value);

      bufferValid = false;
   }

   public void putShortProperty(final String key, final short value)
   {
      properties.putShortProperty(new SimpleString(key), value);

      bufferValid = false;
   }

   public void putIntProperty(final String key, final int value)
   {
      properties.putIntProperty(new SimpleString(key), value);

      bufferValid = false;
   }

   public void putLongProperty(final String key, final long value)
   {
      properties.putLongProperty(new SimpleString(key), value);

      bufferValid = false;
   }

   public void putFloatProperty(final String key, final float value)
   {
      properties.putFloatProperty(new SimpleString(key), value);

      bufferValid = false;
   }

   public void putDoubleProperty(final String key, final double value)
   {
      properties.putDoubleProperty(new SimpleString(key), value);

      bufferValid = false;
   }

   public void putStringProperty(final String key, final String value)
   {
      properties.putSimpleStringProperty(new SimpleString(key), SimpleString.toSimpleString(value));

      bufferValid = false;
   }

   public void putTypedProperties(final TypedProperties otherProps)
   {
      properties.putTypedProperties(otherProps);

      bufferValid = false;
   }

   public Object getObjectProperty(final SimpleString key)
   {
      return properties.getProperty(key);
   }

   public Boolean getBooleanProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      return properties.getBooleanProperty(key);
   }

   public Boolean getBooleanProperty(final String key) throws HornetQPropertyConversionException
   {
      return properties.getBooleanProperty(new SimpleString(key));
   }

   public Byte getByteProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      return properties.getByteProperty(key);
   }

   public Byte getByteProperty(final String key) throws HornetQPropertyConversionException
   {
      return properties.getByteProperty(new SimpleString(key));
   }

   public byte[] getBytesProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      return properties.getBytesProperty(key);
   }

   public byte[] getBytesProperty(final String key) throws HornetQPropertyConversionException
   {
      return getBytesProperty(new SimpleString(key));
   }

   public Double getDoubleProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      return properties.getDoubleProperty(key);
   }

   public Double getDoubleProperty(final String key) throws HornetQPropertyConversionException
   {
      return properties.getDoubleProperty(new SimpleString(key));
   }

   public Integer getIntProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      return properties.getIntProperty(key);
   }

   public Integer getIntProperty(final String key) throws HornetQPropertyConversionException
   {
      return properties.getIntProperty(new SimpleString(key));
   }

   public Long getLongProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      return properties.getLongProperty(key);
   }

   public Long getLongProperty(final String key) throws HornetQPropertyConversionException
   {
      return properties.getLongProperty(new SimpleString(key));
   }

   public Short getShortProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      return properties.getShortProperty(key);
   }

   public Short getShortProperty(final String key) throws HornetQPropertyConversionException
   {
      return properties.getShortProperty(new SimpleString(key));
   }

   public Float getFloatProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      return properties.getFloatProperty(key);
   }

   public Float getFloatProperty(final String key) throws HornetQPropertyConversionException
   {
      return properties.getFloatProperty(new SimpleString(key));
   }

   public String getStringProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      SimpleString str = getSimpleStringProperty(key);

      if (str == null)
      {
         return null;
      }
      else
      {
         return str.toString();
      }
   }

   public String getStringProperty(final String key) throws HornetQPropertyConversionException
   {
      return getStringProperty(new SimpleString(key));
   }

   public SimpleString getSimpleStringProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      return properties.getSimpleStringProperty(key);
   }

   public SimpleString getSimpleStringProperty(final String key) throws HornetQPropertyConversionException
   {
      return properties.getSimpleStringProperty(new SimpleString(key));
   }

   public Object getObjectProperty(final String key)
   {
      return properties.getProperty(new SimpleString(key));
   }

   public Object removeProperty(final SimpleString key)
   {
      bufferValid = false;

      return properties.removeProperty(key);
   }

   public Object removeProperty(final String key)
   {
      bufferValid = false;

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

   public HornetQBuffer getWholeBuffer()
   {
      return buffer;
   }

   public BodyEncoder getBodyEncoder() throws HornetQException
   {
      return new DecodingContext();
   }

   public TypedProperties getTypedProperties()
   {
      return this.properties;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   public TypedProperties getProperties()
   {
      return properties;
   }

   // This must be synchronized as it can be called concurrently id the message is being delivered
   // concurrently to
   // many queues - the first caller in this case will actually encode it
   private synchronized HornetQBuffer encodeToBuffer()
   {
      if (!bufferValid)
      {
         if (bufferUsed)
         {
            // Cannot use same buffer - must copy

            forceCopy();
         }

         int bodySize = getEndOfBodyPosition();

         // write it
         buffer.setInt(BUFFER_HEADER_SPACE, bodySize);

         // Position at end of body and skip past the message end position int.
         // check for enough room in the buffer even though it is dynamic
         if ((bodySize + 4) > buffer.capacity())
         {
            buffer.setIndex(0, bodySize);
            buffer.writeInt(0);
         }
         else
         {
            buffer.setIndex(0, bodySize + DataConstants.SIZE_INT);
         }

         encodeHeadersAndProperties(buffer);

         // Write end of message position

         endOfMessagePosition = buffer.writerIndex();

         buffer.setInt(bodySize, endOfMessagePosition);

         bufferValid = true;
      }

      return buffer;
   }

   private void decode()
   {
      endOfBodyPosition = buffer.getInt(BUFFER_HEADER_SPACE);

      buffer.readerIndex(endOfBodyPosition + DataConstants.SIZE_INT);

      decodeHeadersAndProperties(buffer);

      endOfMessagePosition = buffer.readerIndex();

      bufferValid = true;
   }

   public void createBody(final int initialMessageBufferSize)
   {
      buffer = HornetQBuffers.dynamicBuffer(initialMessageBufferSize);

      // There's a bug in netty which means a dynamic buffer won't resize until you write a byte
      buffer.writeByte((byte)0);

      int limit = BUFFER_HEADER_SPACE + DataConstants.SIZE_INT;

      buffer.setIndex(limit, limit);
   }

   private void forceCopy()
   {
      // Must copy buffer before sending it

      buffer = buffer.copy(0, buffer.capacity());

      buffer.setIndex(0, getEndOfBodyPosition());

      if (bodyBuffer != null)
      {
         bodyBuffer.setBuffer(buffer);
      }

      bufferUsed = false;
   }

   // Inner classes -------------------------------------------------

   private final class DecodingContext implements BodyEncoder
   {
      private int lastPos = 0;

      public DecodingContext()
      {
      }

      public void open()
      {
      }

      public void close()
      {
      }

      public long getLargeBodySize()
      {
         return buffer.writerIndex();
      }

      public int encode(final ByteBuffer bufferRead) throws HornetQException
      {
         HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(bufferRead);
         return encode(buffer, bufferRead.capacity());
      }

      public int encode(final HornetQBuffer bufferOut, final int size)
      {
         bufferOut.writeBytes(getWholeBuffer(), lastPos, size);
         lastPos += size;
         return size;
      }
   }

}
