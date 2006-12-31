/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint.advised;

import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.client.ClientAOPStackProvider;

/**
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class ClientAOPStackProviderAdvised implements ClientAOPStackProvider
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ServerPeer serverPeer;

   // Constructors --------------------------------------------------

   public ClientAOPStackProviderAdvised(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
   }

   // ClientAOPStackProvider implementation --------------------------------

   public byte[] getClientAOPStack()
   {
      return serverPeer.getClientAOPStack();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "ClientAOPStackProviderAdvised->" + serverPeer;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
