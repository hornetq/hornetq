/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.io.Serializable;
import java.util.Set;

/**
 * An atomic, self containted unit of data that flows through the system.
 *
 * It supports the concept of message header. Various messaging system components can attach or
 * remove headers to/from the Routable instances, primarily for message flow management purposes.
 *
 * @see org.jboss.messaging.core.Message
 * @see org.jboss.messaging.core.MessageReference
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Routable extends Serializable
{
   public static final String REMOTE_ROUTABLE = "REMOTE_ROUTABLE";
   // the value is a Serializable
   public static final String REPLICATOR_ID = "REPLICATOR_ID";
   // the value is a Serializable
   public static final String REPLICATOR_INPUT_ID = "REPLICATOR_INPUT_ID";


   /**
    * Returns the ID of the message represented by this Routable.
    */
   public Serializable getMessageID();

   /**
    * Returns true if the delivery is guaranteed for this Routable, false othewise.
    */
   public boolean isReliable();

   /**
    * Returns the time (in GMT milliseconds) at which this Routable expires and must be dissapear
    * from the system. A Long.MAX_VALUE return value means this Routable never expires.
    */
   public long getExpirationTime();

   /**
    * True if the message was delivered at least once but not acknowledged.
    */
   public boolean isRedelivered();

   public void setRedelivered(boolean redelivered);

   /**
    * Associates the specified value with the specified header name. If the header map previously
    * contained a mapping for this name, the old value is replaced by the specified value.
    *
    * @return the value associated with the name or null if there is no mapping for the name. A null
    *         can also indicate that the header map previously associated null with the specified
    *         name.
    */
   public Serializable putHeader(String name, Serializable value);

   /**
    * Returns the value corresponding to the header name. Returns null if the map contains no
    * mapping for the name. A return value of null does not necessarily indicate that the map
    * contains no mapping for the name; it's also possible that the map explicitly maps the name to
    * null. The containsHeader() operation may be used to distinguish these two cases.
    *
    * @return the value associated with the header, or null if there is no mapping for the header.
    */
   public Serializable getHeader(String name);

   /**
    * Removes the header.
    *
    * @return previous value associated with the header, or null if there was no mapping.
    */
   public Serializable removeHeader(String name);

   /**
    * Returns true if the Routable contains the specified header.
    */
   public boolean containsHeader(String name);

   /**
    * Returns a copy of the name set.
    */
   public Set getHeaderNames();

}
