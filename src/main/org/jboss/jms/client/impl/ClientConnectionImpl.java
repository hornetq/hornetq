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
import javax.jms.Session;

import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.client.JBossConnectionMetaData;
import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingMessage;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionResponse;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDRequest;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDResponse;
import org.jboss.messaging.core.remoting.wireformat.SetClientIDMessage;
import org.jboss.messaging.core.remoting.wireformat.StartConnectionMessage;
import org.jboss.messaging.core.remoting.wireformat.StopConnectionMessage;
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

   private volatile boolean closed;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionImpl(String id, int serverID, boolean strictTck, Version version,
                               MessagingRemotingConnection connection)
   {
      this.id = id;
      
      this.serverID = serverID;
      
      this.strictTck = strictTck;
      
      this.versionToUse = version;
      
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
         remotingConnection.send(id, new CloseMessage());
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

         closed = true;
      }
   }

   public synchronized void closing() throws JMSException
   {
      if (closed)
      {
         return;
      }
      
      closeChildren();
      
      remotingConnection.send(id, new ClosingMessage());
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
                                            int acknowledgementMode,
                                            boolean isXA) throws JMSException
   {
      checkClosed();
            
      justCreated = false;

      CreateSessionRequest request = new CreateSessionRequest(transacted, acknowledgementMode, isXA);
      
      CreateSessionResponse response = (CreateSessionResponse)remotingConnection.send(id, request);   
      
      int ackBatchSize;
      
      if (transacted || acknowledgementMode == Session.CLIENT_ACKNOWLEDGE)
      {
         ackBatchSize = -1; //Infinite
      }
      else if (acknowledgementMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         ackBatchSize = response.getDupsOKBatchSize();
      }
      else
      {
         //Auto ack
         ackBatchSize = 1;
      }
       
      ClientSession session =  new ClientSessionImpl(this, response.getSessionID(), ackBatchSize, isXA);
                  
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
         clientID = ((GetClientIDResponse)remotingConnection.send(id, new GetClientIDRequest())).getClientID();
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

      remotingConnection.send(id, new SetClientIDMessage(clientID));  
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
      
      remotingConnection.send(id, new StartConnectionMessage(), true);
   }
   
   public void stop() throws JMSException
   {
      checkClosed();
      
      justCreated = false;
      
      remotingConnection.send(id, new StopConnectionMessage());
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
         session.closing();
         session.close(); 
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
