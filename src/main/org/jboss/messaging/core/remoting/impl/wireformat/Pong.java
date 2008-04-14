/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PONG;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class Pong extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long sessionID;

   private final boolean sessionFailed;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Pong(final long sessionID, final boolean sessionFailed)
   {
      super(PONG);

      this.sessionID = sessionID;
      this.sessionFailed = sessionFailed;
   }

   // Public --------------------------------------------------------

   public long getSessionID()
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
