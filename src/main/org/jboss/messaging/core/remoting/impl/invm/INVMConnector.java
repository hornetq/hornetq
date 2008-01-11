/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.invm;

import static org.jboss.messaging.core.remoting.TransportType.INVM;

import java.io.IOException;

import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.PacketDispatcher;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class INVMConnector implements NIOConnector
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String host;

   private int port;

   private INVMSession session;

   private PacketDispatcher serverDispatcher;
 
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public INVMConnector(String host, int port, PacketDispatcher serverDispatcher)
   {
      assert host != null;
      assert serverDispatcher != null;
      
      this.host = host;
      this.port = port;
      this.serverDispatcher = serverDispatcher;
   }

   // NIOConnector implementation -----------------------------------

   public NIOSession connect()
         throws IOException
   {
      this.session = new INVMSession(serverDispatcher);
      return session;
   }

   public boolean disconnect()
   {
      if (session == null)
      {
         return false;
      } else
      {
         boolean closed = session.close();
         session = null;
         return closed;
      }
   }

   public String getServerURI()
   {
      return INVM + "://" + host + ":" + port;
   }
   
   public void removeConnectionListener(
         ConsolidatedRemotingConnectionListener listener)
   {
   }
   
   public void addConnectionListener(
         ConsolidatedRemotingConnectionListener listener)
   {
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
