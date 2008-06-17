/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.ConcurrentHashSet;

import java.util.HashSet;
import java.util.Set;

/**
 * The client-side Connection connectionFactory class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 * @version <tt>$Revision: 3602 $</tt>
 *
 * $Id: ClientConnectionImpl.java 3602 2008-01-21 17:48:32Z timfox $
 */
public class ClientConnectionImpl implements ClientConnectionInternal
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientConnectionImpl.class);

   private static final boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private final long serverTargetID;
   
   private final RemotingConnection remotingConnection;

   private final Set<ClientSessionInternal> sessions = new ConcurrentHashSet<ClientSessionInternal>();

   private final Version serverVersion;
   
   private final ClientConnectionFactory connectionFactory;
   
   private volatile boolean closed;
      
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionImpl(final ClientConnectionFactory connectionFactory,
                               final long serverTargetID,
                               final RemotingConnection connection,
                               final Version serverVersion)
   {
      this.connectionFactory = connectionFactory;
      
      this.serverTargetID = serverTargetID;
      
      this.remotingConnection = connection;
      
      this.serverVersion = serverVersion;
   }
   
   // ClientConnection implementation --------------------------------------------------------------

   public ClientSession createClientSession(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks,
                                            final int ackBatchSize, final boolean blockOnAcknowledge,
                                            final boolean cacheProducers) throws MessagingException
   {
      checkClosed();

      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(xa, autoCommitSends, autoCommitAcks);

      ConnectionCreateSessionResponseMessage response =
         (ConnectionCreateSessionResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, request);   

      ClientSessionInternal session =
      	new ClientSessionImpl(this, response.getSessionID(), xa, ackBatchSize, cacheProducers,
      			                autoCommitSends, autoCommitAcks, blockOnAcknowledge);

      addSession(session);

      return session;
   }
   
   public ClientSession createClientSession(final boolean xa, final boolean autoCommitSends,
                                            final boolean autoCommitAcks,
                                            final int ackBatchSize) throws MessagingException
   {
      return createClientSession(xa, autoCommitSends, autoCommitAcks, ackBatchSize,
                                 connectionFactory.isDefaultBlockOnAcknowledge(), false);
   }
   
   public void start() throws MessagingException
   {
      checkClosed();
       
      remotingConnection.sendOneWay(serverTargetID, serverTargetID, new PacketImpl(PacketImpl.CONN_START));
   }
   
   public void stop() throws MessagingException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(serverTargetID, serverTargetID, new PacketImpl(PacketImpl.CONN_STOP));
   }

   public void setRemotingSessionListener(final RemotingSessionListener listener) throws MessagingException
   {
      checkClosed();
      
      remotingConnection.setRemotingSessionListener(listener);
   }
   
   public synchronized void close() throws MessagingException
   {
      if (closed)
      {
         return;
      }
      
      try
      {
         closeChildren();
         
         remotingConnection.sendBlocking(serverTargetID, serverTargetID, new PacketImpl(PacketImpl.CLOSE));
      }
      finally
      {
         // Finished with the connection - we need to shutdown callback server
         remotingConnection.stop();

         closed = true;
      }
   }

   public synchronized void cleanUp()
   {
      cleanUpChildren();
      closed = true;
   }

   public boolean isClosed()
   {
      return closed;
   }
   
   // ClientConnectionInternal implementation --------------------------------------------------------
   
   public RemotingConnection getRemotingConnection()
   {
      return remotingConnection;
   }
   
   public void addSession(final ClientSessionInternal session)
   {
      sessions.add(session);
   }
   
   public void removeSession(final ClientSessionInternal session)
   {
      sessions.remove(session);
   }
   
   public Set<ClientSession> getSessions()
   {
      return new HashSet<ClientSession>(this.sessions);
   }

   public Version getServerVersion()
   {
      return serverVersion;
   }
   
   public ClientConnectionFactory getConnectionFactory()
   {
      return connectionFactory;
   }

   // Public ---------------------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
      
   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Connection is closed");
      }
   }
   
   private void closeChildren() throws MessagingException
   {
      //We copy the set of sessions to prevent ConcurrentModificationException which would occur
      //when the child trues to remove itself from its parent
      Set<ClientSession> childrenClone = new HashSet<ClientSession>(sessions);
       
      for (ClientSession session: childrenClone)
      {
         session.close(); 
      }
   }

   private void cleanUpChildren()
   {
      //We copy the set of sessions to prevent ConcurrentModificationException which would occur
      //when the child trues to remove itself from its parent
      Set<ClientSession> childrenClone = new HashSet<ClientSession>(sessions);

      for (ClientSession session: childrenClone)
      {
         session.cleanUp();
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
