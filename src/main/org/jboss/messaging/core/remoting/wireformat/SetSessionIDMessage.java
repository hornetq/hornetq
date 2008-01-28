/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETSESSIONID;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class SetSessionIDMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String sessionID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SetSessionIDMessage(String sessionID)
   {
      super(MSG_SETSESSIONID);

      assertValidID(sessionID);
      this.sessionID = sessionID;
   }

   // Public --------------------------------------------------------

   public String getSessionID()
   {
      return sessionID;
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