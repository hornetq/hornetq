/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class Ping extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long sessionID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Ping(final long sessionID)
   {
      super(PING);
      
      this.sessionID = sessionID;
   }
   
   public Ping()
   {
      super(PING);
   }
      
   // Public --------------------------------------------------------
   
   public long getSessionID()
   {
      return sessionID;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(sessionID);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      sessionID = buffer.getLong();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", sessionID=" + sessionID + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
