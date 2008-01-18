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
package org.jboss.messaging.core;

import java.util.List;
import java.util.Map;

import org.jboss.messaging.util.Streamable;

/**
 * A message is a routable instance that has a payload.
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
   //FIXME - temp
   public static final String TEMP_DEST_HEADER_NAME = "JMSDestination";
   
   
	/**
	 * This header is set on a message when a message is sucked from one node of the cluster to another
	 * and order preservation is true.
	 * The header is checked when sucking messages and if order preservation is true then the message is not accepted.
	 * This is a basic way of ensuring message order is preserved.
	 */
	public static final String CLUSTER_SUCKED = "SUCKED";
	
	/**
	 * This header is set on a message when it is sucked from one node to another.
	 * If the header exists on the destination node, and the message is persistent, the message
	 * will be moved from one channel to the other by doing a simple database update
	 */
	public static final String SOURCE_CHANNEL_ID = "SCID";
	
	/**
	 * The scheduled delivery time for the message
	 */
	public static final String SCHEDULED_DELIVERY_TIME = "SCHED";
	
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
    * 
    * @return List of persisted references for this message
    */
   List<MessageReference> getReferences();
   
   /**
    * 
    * @return The number of durable references for this message
    */
   int getNumDurableReferences();
   
   /**
    * 
    * @param reference durable from the specified position
    * @param pos
    */
   void removeDurableReference(MessageReference reference, int pos) throws Exception;
   
   /**
    * 
    * @param reference
    * @return The position of the reference over all durable references
    */
   int getDurableReferencePos(MessageReference reference);
   
   void addBackDurableReference(MessageReference ref);

   
   /**
    * Make a copy of the message
    * 
    * @return The copy
    */
   Message copy();   
   
   void send() throws Exception;
   
}
