/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.PONG;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class Pong extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String sessionID;

   private final boolean sessionFailed;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Pong(final String sessionID, final boolean sessionFailed)
   {
      super(PONG);

      assertValidID(sessionID);

      this.sessionID = sessionID;
      this.sessionFailed = sessionFailed;
   }

   // Public --------------------------------------------------------

   public String getSessionID()
   {
      return sessionID;
   }

   public boolean isSessionFailed()
   {
      return sessionFailed;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", sessionID=" + sessionID + ", sessionFailed=" + sessionFailed + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
