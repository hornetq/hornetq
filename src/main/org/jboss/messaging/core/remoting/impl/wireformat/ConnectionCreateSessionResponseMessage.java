/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ConnectionCreateSessionResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long sessionTargetID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionCreateSessionResponseMessage(final long sessionTargetID)
   {
      super(PacketType.CONN_CREATESESSION_RESP);

      this.sessionTargetID = sessionTargetID;
   }

   // Public --------------------------------------------------------

   public long getSessionID()
   {
      return sessionTargetID;
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
