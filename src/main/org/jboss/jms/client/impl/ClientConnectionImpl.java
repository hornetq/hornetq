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

import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.client.JBossConnectionMetaData;
import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionResponse;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDRequest;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDResponse;
import org.jboss.messaging.core.remoting.wireformat.GetPreparedTransactionsRequest;
import org.jboss.messaging.core.remoting.wireformat.GetPreparedTransactionsResponse;
import org.jboss.messaging.core.remoting.wireformat.SendTransactionMessage;
import org.jboss.messaging.core.remoting.wireformat.SetClientIDMessage;
import org.jboss.messaging.core.remoting.wireformat.StartConnectionMessage;
import org.jboss.messaging.core.remoting.wireformat.StopConnectionMessage;
import org.jboss.messaging.core.tx.MessagingXid;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.Version;

/**
 * The client-side Connection delegate class.
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
public class ClientConnectionImpl implements ClientConnection
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientConnectionImpl.class);

   private static final boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private String id;
   
   protected JBossConnectionMetaData connMetaData;

   private int serverID;

   private MessagingRemotingConnection remotingConnection;

   private Version versionToUse;
   
   private boolean strictTck;
   
   private Map<String, ClientSession> children = new ConcurrentHashMap<String, ClientSession>();

   private boolean justCreated = true;

   private String clientID;

   private ResourceManager resourceManager;
   
   private volatile boolean closed;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionImpl(String id, int serverID, boolean strictTck, Version version, ResourceManager rm,
                               MessagingRemotingConnection connection)
   {
      this.id = id;
      
      this.serverID = serverID;
      
      this.strictTck = strictTck;
      
      this.versionToUse = version;
      
      this.resourceManager = rm;
      
      this.remotingConnection = connection;
   }

   // Closeable implementation ---------------------------------------------------------------------

   public synchronized void close() throws JMSException
   {
      if (closed)
      {
         return;
      }
      
      try
      {
         remotingConnection.sendBlocking(id, new CloseMessage());
      }
      finally
      {
         // remove the consolidated remoting connection listener

         ConsolidatedRemotingConnectionListener l = remotingConnection.removeConnectionListener();
         
         if (l != null)
         {
            l.clear();
         }

         // Finished with the connection - we need to shutdown callback server
         remotingConnection.stop();

         // And to resource manager
         ResourceManagerFactory.instance.checkInResourceManager(serverID);
         
         closed = true;
      }
   }

   public synchronized long closing(long sequence) throws JMSException
   {
      if (closed)
      {
         return -1;
      }
      
      closeChildren();
      
      ClosingResponse response = (ClosingResponse)remotingConnection.sendBlocking(id, new ClosingRequest(sequence));
      
      return response.getID();
   }
   
   // ClientConnection implementation ------------------------------------------------------------

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public JBossConnectionConsumer createConnectionConsumer(Destination dest,
                                                           String subscriptionName,
                                                           String messageSelector,
                                                           ServerSessionPool sessionPool,
                                                           int maxMessages) throws JMSException
   {
      checkClosed();
      
      return new JBossConnectionConsumer(this, (JBossDestination)dest,
                                         subscriptionName, messageSelector, sessionPool,
                                         maxMessages);
   }



   public ClientSession createClientSession(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA) throws JMSException
   {
      checkClosed();
            
      justCreated = false;

      CreateSessionRequest request = new CreateSessionRequest(transacted, acknowledgmentMode, isXA);
      
      CreateSessionResponse response = (CreateSessionResponse)remotingConnection.sendBlocking(id, request);   
      
      ClientSession session = new ClientSessionImpl(this, response.getSessionID(), response.getDupsOKBatchSize(), isStrictTck(), 
            transacted, acknowledgmentMode, isXA);
      
      children.put(response.getSessionID(), session);
      
      return session;
   }


   public boolean isStrictTck()
   {
      return strictTck;
   }

   public String getClientID() throws JMSException
   {
      checkClosed();
      
      justCreated = false;

      if (clientID == null)
      {
         //Get from the server
         clientID = ((GetClientIDResponse)remotingConnection.sendBlocking(id, new GetClientIDRequest())).getClientID();
      }
      return clientID;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ConnectionMetaData getConnectionMetaData() throws JMSException
   {
      checkClosed();
      
      justCreated = false;

      if (connMetaData == null)
      {
         connMetaData = new JBossConnectionMetaData(versionToUse);
      }

      return connMetaData;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ExceptionListener getExceptionListener() throws JMSException
   {
      justCreated = false;

      return remotingConnection.getConnectionListener().getJMSExceptionListener(); 
   }

   public void sendTransaction(TransactionRequest tr) throws JMSException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(id, new SendTransactionMessage(tr));
   }

   public void setClientID(String clientID) throws JMSException
   {
      checkClosed();
      
      if (this.clientID != null)
      {
         throw new javax.jms.IllegalStateException("Client id has already been set");
      }
      if (!justCreated)
      {
         throw new IllegalStateException("setClientID can only be called directly after the connection is created");
      }

      this.clientID = clientID;
      
      this.justCreated = false;

      remotingConnection.sendBlocking(id, new SetClientIDMessage(clientID));  
   }
   
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setExceptionListener(ExceptionListener listener) throws JMSException
   {
      checkClosed();
      
      justCreated = false;

      remotingConnection.getConnectionListener().addJMSExceptionListener(listener);
   }

   public void start() throws JMSException
   {
      checkClosed();
      
      justCreated = false;
      
      remotingConnection.sendOneWay(id, new StartConnectionMessage());
   }
   
   public void stop() throws JMSException
   {
      checkClosed();
      
      justCreated = false;
      
      remotingConnection.sendBlocking(id, new StopConnectionMessage());
   }

   public MessagingXid[] getPreparedTransactions() throws JMSException
   {
      checkClosed();
      
      GetPreparedTransactionsResponse response =
         (GetPreparedTransactionsResponse)remotingConnection.sendBlocking(id, new GetPreparedTransactionsRequest());
      
      return response.getXids();
   }
   
   public ResourceManager getResourceManager()
   {
      return resourceManager;
   }
   
   public MessagingRemotingConnection getRemotingConnection()
   {
      return remotingConnection;
   }

   public int getServerID()
   {
      return serverID;
   }
   
   public void removeChild(String key) throws JMSException
   {
      checkClosed();
      
      children.remove(key);
   }

   // Public ---------------------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
      
   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void checkClosed() throws IllegalStateException
   {
      if (closed)
      {
         throw new IllegalStateException("Connection is closed");
      }
   }
   
   private void closeChildren() throws JMSException
   {
      //We copy the set of children to prevent ConcurrentModificationException which would occur
      //when the child trues to remove itself from its parent
      Set<ClientSession> childrenClone = new HashSet<ClientSession>(children.values());
      
      for (ClientSession session: childrenClone)
      {
         session.closing(-1);
         session.close(); 
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
