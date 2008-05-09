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
public class ConnectionCreateSessionResponseMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long sessionTargetID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionCreateSessionResponseMessage(final long sessionTargetID)
   {
      super(CONN_CREATESESSION_RESP);

      this.sessionTargetID = sessionTargetID;
   }
   
   public ConnectionCreateSessionResponseMessage()
   {
      super(CONN_CREATESESSION_RESP);
   }

   // Public --------------------------------------------------------

   public long getSessionID()
   {
      return sessionTargetID;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(sessionTargetID);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      sessionTargetID = buffer.getLong();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", sessionTargetID=" + sessionTargetID
            + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
