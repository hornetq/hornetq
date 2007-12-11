/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETCLIENTID;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class SetClientIDMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String clientID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SetClientIDMessage(String clientID)
   {
      super(MSG_SETCLIENTID);

      assertValidID(clientID);

      this.clientID = clientID;
   }

   // Public --------------------------------------------------------

   public String getClientID()
   {
      return clientID;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", clientID=" + clientID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
