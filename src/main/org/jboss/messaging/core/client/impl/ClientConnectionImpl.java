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

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_STOP;

import java.util.HashSet;
import java.util.Set;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.ConcurrentHashSet;

/**
 * The client-side Connection connectionFactory class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
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

   private final boolean strictTck;
   
   private final Set<ClientSession> sessions = new ConcurrentHashSet<ClientSession>();

   private volatile boolean closed;
   
   private final int defaultConsumerWindowSize;
   
   private final int defaultConsumerMaxRate;
   
   private final int defaultProducerWindowSize;
   
   private final int defaultProducerMaxRate;

   private final Version serverVersion;
   

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionImpl(final long serverTargetID,
   									 final boolean strictTck,
                               final RemotingConnection connection,
                               final int defaultConsumerWindowSize,     
                               final int defaultConsumerMaxRate,
                               final int defaultProducerWindowSize,
                               final int defaultProducerMaxRate,
                               final Version serverVersion)
   {
      this.serverTargetID = serverTargetID;
      
      this.strictTck = strictTck;
      
      this.remotingConnection = connection;
      
      this.defaultConsumerWindowSize = defaultConsumerWindowSize;
      
      this.defaultConsumerMaxRate = defaultConsumerMaxRate;
      
      this.defaultProducerWindowSize = defaultProducerWindowSize;
      
      this.defaultProducerMaxRate = defaultProducerMaxRate;

      this.serverVersion = serverVersion;
   }
   
   // ClientConnection implementation --------------------------------------------------------------

   public ClientSession createClientSession(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks,
                                            final int ackBatchSize, final boolean blockOnAcknowledge,
                                            final boolean cacheProducers) throws MessagingException
   {
      checkClosed();

      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(xa, autoCommitSends, autoCommitAcks);

      ConnectionCreateSessionResponseMessage response = (ConnectionCreateSessionResponseMessage)remotingConnection.send(serverTargetID, request);   

      ClientSession session =
      	new ClientSessionImpl(this, response.getSessionID(), ackBatchSize, cacheProducers,
      			autoCommitSends, autoCommitAcks, blockOnAcknowledge,
      			defaultConsumerWindowSize, defaultConsumerMaxRate, defaultProducerWindowSize, defaultProducerMaxRate);

      sessions.add(session);

      return session;
   }
   
   public void start() throws MessagingException
   {
      checkClosed();
       
      remotingConnection.send(serverTargetID, serverTargetID, new PacketImpl(CONN_START), true);
   }
   
   public void stop() throws MessagingException
   {
      checkClosed();
      
      remotingConnection.send(serverTargetID, new PacketImpl(CONN_STOP));
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
         
         remotingConnection.send(serverTargetID, new PacketImpl(CLOSE));
      }
      finally
      {
         // Finished with the connection - we need to shutdown callback server
         remotingConnection.stop();

         closed = true;
      }
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
   
   public void removeSession(final ClientSession session)
   {
      sessions.remove(session);
   }

   public Version getServerVersion()
   {
      return serverVersion;
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

   // Inner Classes --------------------------------------------------------------------------------

}
