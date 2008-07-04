/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.core.remoting.impl.invm;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnector;
import org.jboss.messaging.core.remoting.RemotingSession;
import org.jboss.messaging.util.ByteBufferWrapper;
import org.jboss.messaging.util.MessagingBuffer;

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

   // RemotingConnector implementation -----------------------------------

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
   
   public MessagingBuffer createBuffer(int size)
   {
      return new ByteBufferWrapper(ByteBuffer.allocate(size));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
