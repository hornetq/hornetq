/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.Routable;

import java.io.Serializable;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

/**
 * Contains the plumbing of a Routable.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RoutableSupport implements Routable
{

   // Attributes ----------------------------------------------------

   protected Serializable messageID;
   protected boolean reliable;
   protected long expirationTime;
   protected Map headers;

   // Constructors --------------------------------------------------

   /**
    * Constructs a generic Routable that is not reliable and does not expire.
    */
   public RoutableSupport(Serializable messageID)
   {
      this(messageID, false, 0);
   }

   /**
    * Constructs a generic Routable that does not expire.
    */
   public RoutableSupport(Serializable messageID, boolean reliable)
   {
      this(messageID, reliable, 0);
   }

   public RoutableSupport(Serializable messageID, boolean reliable, long expirationTime)
   {
      this.messageID = messageID;
      this.reliable = reliable;
      this.expirationTime = expirationTime;
      headers = new HashMap();
   }

   // Routable implementation ---------------------------------------

   public Serializable getMessageID()
   {
      return messageID;
   }

   public boolean isReliable()
   {
      return reliable;
   }

   public long getExpirationTime()
   {
      return expirationTime;
   }

   public Serializable putHeader(String name, Serializable value)
   {
      return (Serializable)headers.put(name, value);
   }

   public Serializable getHeader(String name)
   {
      return (Serializable)headers.get(name);
   }

   public Serializable removeHeader(String name)
   {
      return (Serializable)headers.remove(name);
   }

   public boolean containsHeader(String name)
   {
      return headers.containsKey(name);
   }

   public Set getHeaderNames()
   {
      return headers.keySet();
   }

}
