/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.io.Serializable;
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
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Routable extends Serializable
{
   static final String REMOTE_ROUTABLE = "REMOTE_ROUTABLE";
   static final String REPLICATOR_ID = "REPLICATOR_ID";
   static final String REPLICATOR_INPUT_ID = "REPLICATOR_INPUT_ID";

   Serializable getMessageID();

   /**
    * If it is a Message instance, then it returns itself, otherwise it will return the Message
    * corresponding to this MessageReference.
    */
   Message getMessage();

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

   /**
    * @return true if the delivery of this message had to be repeated at least once.
    */
   boolean isRedelivered();

   void setRedelivered(boolean redelivered);

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

}
