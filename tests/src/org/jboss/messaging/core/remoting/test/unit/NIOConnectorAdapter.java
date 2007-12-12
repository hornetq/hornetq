/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.test.unit;

import java.io.IOException;

import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.TransportType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class NIOConnectorAdapter implements NIOConnector
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // NIOConnector implementation -----------------------------------
   
   public void addConnectionListener(
         ConsolidatedRemotingConnectionListener listener)
   {
   }

   public NIOSession connect(String host, int port, TransportType transport)
         throws IOException
   {
      return null;
   }

   public boolean disconnect()
   {
      return false;
   }

   public String getServerURI()
   {
      return null;
   }

   public void removeConnectionListener(
         ConsolidatedRemotingConnectionListener listener)
   {
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
