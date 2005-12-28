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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * An atomic, self containted unit of data that is being routed by the messaging system.
 *
 * Each routable maintains a set of headers. Various messaging components can attach or remove
 * headers, primarily for message flow management purposes.
 *
 * @see org.jboss.messaging.core.Message
 * @see org.jboss.messaging.core.MessageReference
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Routable extends Serializable
{
   static final String REMOTE_ROUTABLE = "REMOTE_ROUTABLE";
   static final String REPLICATOR_ID = "REPLICATOR_ID";
   static final String COLLECTOR_ID = "COLLECTOR_ID";

   Serializable getMessageID();

   /**
    * If it is a Message instance, then it returns itself, otherwise it will return the Message
    * corresponding to this MessageReference.
    */
   Message getMessage(); //FIXME Shouldn't this method be on MessageReference interface rather than this interface??

   boolean isReference();

   /**
    * @return true if the delivery must be guaranteed for this routable, false othewise.
    */
   boolean isReliable();

   /**
    * @return the time (in GMT milliseconds) when this routable expires and must be removed
    *         from the system. A zero value means this routable never expires.
    */
   long getExpiration();

   boolean isExpired();

   /**
    * @return the time (in GMT milliseconds) when this routable was delivered to the provider.
    */
   long getTimestamp();
   
   int getPriority();
   
   void setPriority(int priority);

   /**
    * @return true if the delivery of this message had to be repeated at least once.
    */
   boolean isRedelivered();

   void setRedelivered(boolean redelivered);
   
   /**
    * @return the number of times delivery has been attempted for this routable
    */
   int getDeliveryCount();
   
   void setDeliveryCount(int deliveryCount);
   
   void incrementDeliveryCount();
   
   /**
    * @return the ordering of the message. Needed when it is required to preserve some kind
    * of order across messages sends
    */
   long getOrdering();

   /**
    * Binds a header. If the header map previously contained a mapping for this name, the old value
    * is replaced by the specified value.
    *
    * @return the value associated with the name or null if there is no mapping for the name. A null
    *         can also indicate that the header map previously associated null with the specified
    *         name.
    */
   Serializable putHeader(String name, Serializable value);

   /**
    * Returns the value corresponding to the header name. Returns null if the map contains no
    * mapping for the name. A return value of null does not necessarily indicate that the map
    * contains no mapping for the name; it's also possible that the map explicitly maps the name to
    * null. The containsHeader() operation may be used to distinguish these two cases.
    *
    * @return the value associated with the header, or null if there is no mapping for the header.
    */
   Serializable getHeader(String name);

   /**
    * Removes the header.
    *
    * @return previous value associated with the header, or null if there was no mapping.
    */
   Serializable removeHeader(String name);

   /**
    * Returns true if the Routable contains the specified header.
    */
   boolean containsHeader(String name);

   /**
    * Returns a copy of the header name set.
    */
   Set getHeaderNames();
   
   Map getHeaders();

}
