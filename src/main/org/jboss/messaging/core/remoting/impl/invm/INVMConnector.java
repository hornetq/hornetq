/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.invm;

import java.io.IOException;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnector;
import org.jboss.messaging.core.remoting.RemotingSession;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class INVMConnector implements RemotingConnector
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private INVMSession session;

   private final long id;
      
   private final PacketDispatcher clientDispatcher;
   private final PacketDispatcher serverDispatcher;
   
   private final Location location;
   private final ConnectionParams connectionParams;
     
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public INVMConnector(final Location location, final ConnectionParams params,
                        final long id, final PacketDispatcher clientDispatcher, final PacketDispatcher serverDispatcher)
   {
   	this.id = id;
      this.clientDispatcher = clientDispatcher;
      this.serverDispatcher = serverDispatcher;
      this.location = location;
      this.connectionParams = params;
   }

   // Public --------------------------------------------------------

   // NIOConnector implementation -----------------------------------

   public RemotingSession connect()
         throws IOException
   {
      this.session = new INVMSession(id, clientDispatcher, serverDispatcher);
      return session;
   }

   public boolean disconnect()
   {
      if (session == null)
      {
         return false;
      }
      else
      {
         boolean closed = session.close();
         session = null;
         return closed;
      }
   }

   public Location getLocation()
   {
      return location;
   }
   
   public ConnectionParams getConnectionParams()
   {
      return connectionParams;
   }

   public PacketDispatcher getDispatcher()
   {
      return clientDispatcher;
   }

   public void addSessionListener(RemotingSessionListener listener)
   {      
   }
   
   public void removeSessionListener(RemotingSessionListener listener)
   {      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
