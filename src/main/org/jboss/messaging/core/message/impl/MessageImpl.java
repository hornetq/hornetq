/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.remoting.impl.mina.BufferWrapper;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TypedProperties;

/**
 * A concrete implementation of a message
 * 
 * All messages handled by JBM core are of this type
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2740 $</tt>
 * 
 * For normal message transportation serialization is not used
 * 
 * $Id: MessageSupport.java 2740 2007-05-30 11:36:28Z timfox $
 */
public class MessageImpl implements Message
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(MessageImpl.class);

   // Attributes ----------------------------------------------------

   private SimpleString destination;
   
   private long messageID;
   
   private int type;
   
   private boolean durable;

   /** GMT milliseconds at which this message expires. 0 means never expires * */
   private long expiration;

   private long timestamp;

   private TypedProperties properties;
   
   private byte priority;

   private long connectionID;
   
   private final AtomicInteger durableRefCount = new AtomicInteger(0);
   
   private int deliveryCount;
   
   private long deliveryID;
   
   private MessagingBuffer body;
   
   // Constructors --------------------------------------------------

   /*
    * Construct when reading from network
    */
   public MessageImpl()
   {
      this.properties = new TypedProperties();
   }

   /*
    * Construct a message before sending
    */
   public MessageImpl(final int type, final boolean durable, final long expiration,
                      final long timestamp, final byte priority)
   {
      this();
      this.type = type;
      this.durable = durable;
      this.expiration = expiration;
      this.timestamp = timestamp;
      this.priority = priority;            
      this.body = new BufferWrapper(1024);
   }

   /*
    * Construct a MessageImpl from storage
    */
   public MessageImpl(final long messageID)
   {
      this();
      this.messageID = messageID;      
   }
   
   /*
    * Copy constructor
    */
   public MessageImpl(final MessageImpl other)
   {
      this.destination = other.destination;
      this.messageID = other.messageID;
      this.type = other.type;
      this.durable = other.durable;
      this.expiration = other.expiration;
      this.timestamp = other.timestamp;
      this.priority = other.priority;
      this.properties = new TypedProperties(other.properties);
      this.body = other.body;
      
      this.deliveryCount = other.deliveryCount;
      this.deliveryID = other.deliveryID;
   }
   
   // Message implementation ----------------------------------------

   public MessagingBuffer encode()
   {
      MessagingBuffer buff = new BufferWrapper(1024);
      
      buff.putSimpleString(destination);
      buff.putInt(type);
      buff.putBoolean(durable);
      buff.putLong(expiration);
      buff.putLong(timestamp);
      buff.putByte(priority);
      
      buff.putInt(deliveryCount);
      buff.putLong(deliveryID);
      
      properties.encode(buff);
                       
      buff.putInt(body.limit());
      
      //TODO this can be optimisied
      buff.putBytes(body.array(), 0, body.limit());
      
      return buff;
   }
   
   public void decode(final MessagingBuffer buffer)
   {
      destination = buffer.getSimpleString();
      type = buffer.getInt();
      durable = buffer.getBoolean();
      expiration = buffer.getLong();
      timestamp = buffer.getLong();
      priority = buffer.getByte();
      
      deliveryCount = buffer.getInt();
      deliveryID = buffer.getLong();
      
      properties.decode(buffer);
      int len = buffer.getInt();
      
      //TODO - this can be optimised
      byte[] bytes = new byte[len];
      buffer.getBytes(bytes);
      body = new BufferWrapper(1024);
      body.putBytes(bytes);      
   }
   
   public SimpleString getDestination()
   {
      return destination;
   }
   
   public void setDestination(SimpleString destination)
   {
      this.destination = destination;
   }
   
   public long getMessageID()
   {
      return messageID;
   }
   
   public void setMessageID(final long id)
   {
      this.messageID = id;
   }
   
   public int getType()
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
     
   public long getConnectionID()
   {
      return connectionID;
   }
   
   public void setConnectionID(final long connectionID)
   {
      this.connectionID = connectionID;
   }
   
   public void setDeliveryCount(final int deliveryCount)
   {
      this.deliveryCount = deliveryCount;
   }
   
   public int getDeliveryCount()
   {
      return this.deliveryCount;
   }
   
   public void setDeliveryID(final long deliveryID)
   {
      this.deliveryID = deliveryID;
   }
   
   public long getDeliveryID()
   {
      return this.deliveryID;
   }

   public boolean isExpired()
   {
      if (expiration == 0)
      {
         return false;
      }
      
      return System.currentTimeMillis() - expiration >= 0;
   }
   
   public Message copy()
   {
      return new MessageImpl(this);
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
   
   
   // TODO Other stuff that should be moved to ServerMessage:
   // -------------------------------------------------------
   
   public MessageReference createReference(final Queue queue)
   {
      MessageReference ref = new MessageReferenceImpl(this, queue);
      
      //references.add(ref);
      
      if (durable && queue.isDurable())
      {
         durableRefCount.incrementAndGet();
      }
      
      return ref;
   }
   
   public int getDurableRefCount()
   {
   	return durableRefCount.get();
   }
   
   public void decrementDurableRefCount()
   {
   	durableRefCount.decrementAndGet();
   }
   
   public void incrementDurableRefCount()
   {
   	durableRefCount.incrementAndGet();
   }
     
   // Public --------------------------------------------------------

   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      
      if (!(o instanceof MessageImpl))
      {
         return false;
      }
      
      MessageImpl that = (MessageImpl) o;
      
      return that.messageID == this.messageID;
   }

   public int hashCode()
   {
      return (int) ((this.messageID >>> 32) ^ this.messageID);
   }

   public String toString()
   {
      return "M[" + messageID + "]@" + System.identityHashCode(this);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------  
}
