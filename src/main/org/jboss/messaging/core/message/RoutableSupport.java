/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.message;

import org.jboss.messaging.core.Routable;
import org.jboss.logging.Logger;

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

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RoutableSupport.class);

   // Attributes ----------------------------------------------------

   protected Serializable messageID;
   protected boolean reliable;
   /** GMT milliseconds at which this message expires. 0 means never expires **/
   protected long expiration;
   protected long timestamp;
   protected Map headers;
   protected boolean redelivered;

   // Constructors --------------------------------------------------

   /**
    * Constructs a generic Routable that is not reliable and does not expire.
    */
   public RoutableSupport(Serializable messageID)
   {
      this(messageID, false, Long.MAX_VALUE);
   }

   /**
    * Constructs a generic Routable that does not expire.
    */
   public RoutableSupport(Serializable messageID, boolean reliable)
   {
      this(messageID, reliable, Long.MAX_VALUE);
   }

   public RoutableSupport(Serializable messageID, boolean reliable, long timeToLive)
   {
      this.messageID = messageID;
      this.reliable = reliable;
      timestamp = System.currentTimeMillis();
      expiration = timeToLive == Long.MAX_VALUE ? 0 : timestamp + timeToLive;
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

   public long getExpiration()
   {
      return expiration;
   }

   public long getTimestamp()
   {
      return timestamp;
   }

   public boolean isRedelivered()
   {
      return redelivered;
   }

   public void setRedelivered(boolean redelivered)
   {
      this.redelivered = redelivered;
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

   // Public --------------------------------------------------------

   public boolean isExpired()
   {
      if (expiration == 0)
      {
         return false;
      }
      long overtime = System.currentTimeMillis() - expiration;
      if (overtime >= 0)
      {
         // discard it
         log.debug("Message " + messageID + " expired by " + overtime + " ms");
         return true;
      }
      return false;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("RoutableSupport[");
      sb.append(messageID);
      sb.append("]");
      return sb.toString();
   }


}
