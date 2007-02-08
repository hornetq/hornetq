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

import java.util.Map;
import java.util.Set;

import org.jboss.messaging.util.Streamable;

/**
 * A message is a routable instance that has a payload. The payload is opaque to the messaging
 * system.
 *
 * When implementing this interface, make sure you override equals() and hashCode() such that two
 * Message instances with equals IDs are equal.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox"jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Message extends Streamable
{
   static final String FAILED_NODE_ID = "FAILED_NODE_ID";

   long getMessageID();

   /**
    * @return true if the delivery must be guaranteed for this routable, false otherwise.
    */
   boolean isReliable();
   
   /**
    * @return the time (in GMT milliseconds) when this routable expires and must be removed
    *         from the system. A zero value means this routable never expires.
    */
   long getExpiration();

   boolean isExpired();
   
   void setExpiration(long expiration);
   
   /**
    * @return the time (in GMT milliseconds) when this routable was delivered to the provider.
    */
   long getTimestamp();
   
   byte getPriority();
   
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
   
   void setHeaders(Map headers);

   /**
    * Returns a copy of the header name set.
    */
   Set getHeaderNames();
   
   Map getHeaders();
   
   Object getPayload();
   
   byte[] getPayloadAsByteArray();
    
   boolean isPersisted();
   
   void setPersisted(boolean persisted);
   
   byte getType();
   
}
