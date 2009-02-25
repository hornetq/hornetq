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

package org.jboss.messaging.core.message;

import java.util.Set;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.TypedProperties;

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

   void encode(MessagingBuffer buffer);
   
   void decode(MessagingBuffer buffer);
   
   
   int getPropertiesEncodeSize();
   
   void encodeProperties(MessagingBuffer buffer);
   
   void decodeProperties(MessagingBuffer buffer);
      
   int getBodySize();
         
   // Used on Message chunk
   void encodeBody(MessagingBuffer buffer, long start, int size);
   
   void encodeBody(MessagingBuffer buffer);
   
   void decodeBody(MessagingBuffer buffer);
      
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
   
   void putTypedProperties(TypedProperties properties);

   // TODO - should have typed property getters and do conversions herein
   
   Object getProperty(SimpleString key);
   
   Object removeProperty(SimpleString key);
   
   boolean containsProperty(SimpleString key);
   
   Set<SimpleString> getPropertyNames();
   
   // Body
   // ---------------------------------------------------------------------------------
   
   MessagingBuffer getBody();
   
   void setBody(MessagingBuffer body);
   
}
