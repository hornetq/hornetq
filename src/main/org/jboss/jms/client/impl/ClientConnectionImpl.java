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
package org.jboss.jms.client.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.api.FailureListener;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionStartMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionStopMessage;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

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

   private String id;
   
   private int serverID;

   private MessagingRemotingConnection remotingConnection;

   private boolean strictTck;
   
   private Map<String, ClientSession> children = new ConcurrentHashMap<String, ClientSession>();

   private volatile boolean closed;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionImpl(String id, int serverID, boolean strictTck,
                               MessagingRemotingConnection connection)
   {
      this.id = id;
      
      this.serverID = serverID;
      
      this.strictTck = strictTck;
      
      this.remotingConnection = connection;
   }
   
   // ClientConnection implementation --------------------------------------------------------------

   public ClientSession createClientSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks,
                                            int ackBatchSize) throws MessagingException
   {
      checkClosed();

      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(xa, autoCommitSends, autoCommitAcks);

      ConnectionCreateSessionResponseMessage response = (ConnectionCreateSessionResponseMessage)remotingConnection.send(id, request);   

      ClientSession session =  new ClientSessionImpl(this, response.getSessionID(), ackBatchSize);

      children.put(response.getSessionID(), session);

      return session;
   }
   
   public void start() throws MessagingException
   {
      checkClosed();
       
      remotingConnection.send(id, new ConnectionStartMessage(), true);
   }
   
   public void stop() throws MessagingException
   {
      checkClosed();
      
      remotingConnection.send(id, new ConnectionStopMessage());
   }

   public void setFailureListener(FailureListener listener) throws MessagingException
   {
      checkClosed();
      
      remotingConnection.setFailureListener(listener);
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
         
         remotingConnection.send(id, new CloseMessage());
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
   
   public int getServerID()
   {
      return serverID;
   }
   
   public MessagingRemotingConnection getRemotingConnection()
   {
      return remotingConnection;
   }
   
   public void removeChild(String key)
   {
      children.remove(key);
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
      //We copy the set of children to prevent ConcurrentModificationException which would occur
      //when the child trues to remove itself from its parent
      Set<ClientSession> childrenClone = new HashSet<ClientSession>(children.values());
      
      for (ClientSession session: childrenClone)
      {
         session.close(); 
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
