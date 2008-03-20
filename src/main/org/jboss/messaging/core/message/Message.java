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
package org.jboss.messaging.core.message;

import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.util.Streamable;

/**
 * A message is a routable instance that has a payload.
 * 
 * The payload is opaque to the messaging system.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox"jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 3341 $</tt>
 *
 * $Id: Message.java 3341 2007-11-19 14:34:57Z timfox $
 */
public interface Message extends Streamable
{
   public static final String HDR_ACTUAL_EXPIRY_TIME = "JBMActualExpiryTime";
   
   /**    
    * @return The unique id of the message
    */
   long getMessageID();
   
   /**
    * Set the message id
    * 
    * @param id
    */
   void setMessageID(long id);

   /**
    * @return Whether the message is durable
    */
   boolean isDurable();
   
   /**
    * Set whether message is durable
    * @param reliable
    */
   void setDurable(boolean durable);
   
   /**
    * @return the time when this routable expires and must be removed
    *         from the system. A zero value means this routable never expires.
    */
   long getExpiration();

   /**
    * 
    * @return true if the message has expired
    */
   boolean isExpired();
   
   /**
    * Set the expiration for this message
    * 
    * @param expiration
    */
   void setExpiration(long expiration);
   
   /**
    * @return the time (in GMT milliseconds) when this routable was delivered to the provider.
    */
   long getTimestamp();
   
   /**
    * Set the timestamp for this message
    * @param timestamp The timestamp
    */
   void setTimestamp(long timestamp);
   
   /**
    * 
    * @return The priority (0-9) of the message
    */
   byte getPriority();
   
   /**
    * Get the priority of the message. Priorities range from 0 to 9.
    * Where 0 is the lowest priority and 9 is the highest priority
    * @param priority
    */
   void setPriority(byte priority);

   /**
    * Binds a header. If the header map previously contained a mapping for this name, the old value
    * is replaced by the specified value.
    *
    * @return the value associated with the name or null if there is no mapping for the name. A null
    *         can also indicate that the header map previously associated null with the specified
    *         name.
    */
   Object putHeader(String name, Object value);

   /**
    * Returns the value corresponding to the header name. Returns null if the map contains no
    * mapping for the name. A return value of null does not necessarily indicate that the map
    * contains no mapping for the name; it's also possible that the map explicitly maps the name to
    * null. The containsHeader() operation may be used to distinguish these two cases.
    *
    * @return the value associated with the header, or null if there is no mapping for the header.
    */
   Object getHeader(String name);

   /**
    * Removes the header.
    *
    * @return previous value associated with the header, or null if there was no mapping.
    */
   Object removeHeader(String name);

   /**
    * Returns true if the Routable contains the specified header.
    */
   boolean containsHeader(String name);
      
   /**
    * 
    * @return The message's headers
    */
   Map<String, Object> getHeaders();
   
   /**
    * 
    * @return The message's payload
    */
   byte[] getPayload();
   
   
   /**
    * Set the payload
    * 
    * @param payload
    */
   void setPayload(byte[] payload);
   
   /**
    * 
    * @return The message's headers as byte array
    */
   byte[] getHeaderBytes() throws Exception;
    
   /**
    * 
    * @return the type of the message
    */
   int getType();   
   
   /**
    * 
    * @return The delivery count of the message - only available on the client side
    */
   int getDeliveryCount();
   
   /**
    * Set the delivery count of the message
    * @param count
    */
   void setDeliveryCount(int count);
   
   /**
    * Get the connection id
    * @return
    */
   String getConnectionID();
   
   /**
    * Set the connection id
    * @param connectionID
    */
   void setConnectionID(String connectionID);
   
   
   /**
    * @return a reference for this message
    */
   MessageReference createReference(Queue queue);   
   
   /**
    * Decrement the durable ref count
    */
   void decrementDurableRefCount();
   
   /**
    * Increment the durable ref count
    */
   void incrementDurableRefCount();
   
   /**
    * Get the current durable reference count
    * @return
    */
   int getDurableRefCount();
   
   /**
    * Make a copy of the message
    * 
    * @return The copy
    */
   Message copy();   
    
}
