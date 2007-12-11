/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATECONNECTION;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateConnectionResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String connectionID;
   private final int serverID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConnectionResponse(String connectionID, int serverID)
   {
      super(RESP_CREATECONNECTION);

      assertValidID(connectionID);

      this.connectionID = connectionID;
      this.serverID = serverID;
   }

   // Public --------------------------------------------------------

   public String getConnectionID()
   {
      return connectionID;
   }
   
   public int getServerID()
   {
      return serverID;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", id=" + connectionID + ", serverID=" + serverID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
