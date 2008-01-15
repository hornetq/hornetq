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
package org.jboss.jms.client.delegate;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

import org.jboss.jms.client.FailoverListener;
import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.client.JBossConnectionMetaData;
import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.tx.MessagingXid;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.logging.Logger;
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
import org.jboss.messaging.util.ProxyFactory;
import org.jboss.messaging.util.Version;

/**
 * The client-side Connection delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientConnectionDelegate extends DelegateSupport<ConnectionState> implements ConnectionDelegate
{
   // Constants ------------------------------------------------------------------------------------

	private static final long serialVersionUID = -5485083713058725777L;

	private static final Logger log = Logger.getLogger(ClientConnectionDelegate.class);

   private static final boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   protected JBossConnectionMetaData connMetaData;

   private int serverID;

   private transient JMSRemotingConnection remotingConnection;

   private transient Version versionToUse;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionDelegate(String objectID, int serverID)
   {
      super(objectID);

      this.serverID = serverID;
   }

   public ClientConnectionDelegate()
   {
   }

   // DelegateSupport overrides --------------------------------------------------------------------

   public void synchronizeWith(DelegateSupport nd) throws Exception
   {
      log.trace(this + " synchronizing with " + nd);

      super.synchronizeWith(nd);

      ClientConnectionDelegate newDelegate = (ClientConnectionDelegate)nd;

      // synchronize the server endpoint state

      // this is a bit counterintuitve, as we're not copying from new delegate, but modifying its
      // state based on the old state. It makes sense, since in the end the state makes it to the
      // server

      ConnectionState thisState = (ConnectionState)state;

      if (thisState.getClientID() != null)
      {
         newDelegate.setClientID(thisState.getClientID());
      }

      // synchronize (recursively) the client-side state

      state.synchronizeWith(newDelegate.getState());

      // synchronize the delegates

      remotingConnection = newDelegate.getRemotingConnection();
      versionToUse = newDelegate.getVersionToUse();

      // There is one RM per server, so we need to merge the rms if necessary
      ResourceManagerFactory.instance.handleFailover(serverID, newDelegate.getServerID());

      client = remotingConnection.getRemotingClient();

      serverID = newDelegate.getServerID();
   }

   public void setState(ConnectionState state)
   {
      super.setState(state);

      client = state.getRemotingConnection().getRemotingClient();
   }

   // Closeable implementation ---------------------------------------------------------------------

   public void close() throws JMSException
   {
      try
      {
         sendBlocking(new CloseMessage());
      }
      finally
      {
         //Always cleanup in a finally - we need to cleanup if the server call to close fails too

         JMSRemotingConnection remotingConnection = state.getRemotingConnection();

         // remove the consolidated remoting connection listener

         ConsolidatedRemotingConnectionListener l = remotingConnection.removeConnectionListener();
         if (l != null)
         {
            l.clear();
         }

         // Finished with the connection - we need to shutdown callback server
         remotingConnection.stop();

         // And to resource manager
         ResourceManagerFactory.instance.checkInResourceManager(state.getServerID());
      }

   }

   public long closing(long sequence) throws JMSException
   {
      closeChildren();
      ClosingResponse response = (ClosingResponse) sendBlocking(new ClosingRequest(sequence));
      return response.getID();
   }

   // ConnectionDelegate implementation ------------------------------------------------------------

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
      if (trace) { log.trace("createConnectionConsumer()"); }


      return new JBossConnectionConsumer((ConnectionDelegate)ProxyFactory.proxy(this, ConnectionDelegate.class), (JBossDestination)dest,
                                         subscriptionName, messageSelector, sessionPool,
                                         maxMessages);
   }



   private SessionState createSessionData(ClientSessionDelegate sessionDelegate, SessionDelegate proxyDelegate, boolean transacted, int ackMode, boolean xa)
   {

      ConnectionState connectionState = getState();

      SessionState sessionState =
         new SessionState(connectionState, sessionDelegate, proxyDelegate, transacted,
                          ackMode, xa, sessionDelegate.getDupsOKBatchSize());

      return sessionState;
   }


   public SessionDelegate createSessionDelegate(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA) throws JMSException
   {

      state.setJustCreated(false);


      CreateSessionRequest request = new CreateSessionRequest(transacted, acknowledgmentMode, isXA);
      CreateSessionResponse response = (CreateSessionResponse) sendBlocking(request);         
      ClientSessionDelegate delegate = new ClientSessionDelegate(response.getSessionID(), response.getDupsOKBatchSize(), response.isStrictTCK());

      SessionDelegate proxy =(SessionDelegate) ProxyFactory.proxy(delegate, SessionDelegate.class);
      delegate.setState(createSessionData(delegate, proxy, transacted, acknowledgmentMode, isXA));
      return proxy;
   }


   public String getClientID() throws JMSException
   {
      ConnectionState currentState = getState();

      currentState.setJustCreated(false);

      if (currentState.getClientID() == null)
      {
         //Get from the server
         currentState.setClientID(invokeGetClientID());
      }
      return currentState.getClientID();

   }

   private String invokeGetClientID() throws JMSException
   {
      GetClientIDResponse response = (GetClientIDResponse) sendBlocking(new GetClientIDRequest());
      return response.getClientID();
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ConnectionMetaData getConnectionMetaData() throws JMSException
   {
      ConnectionState currentState = getState();
      currentState.setJustCreated(false);

      if (connMetaData == null)
      {
         connMetaData = new JBossConnectionMetaData(getState().getVersionToUse());
      }

      return connMetaData;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ExceptionListener getExceptionListener() throws JMSException
   {
      state.setJustCreated(false);

      return state.getRemotingConnection().getConnectionListener().getJMSExceptionListener();
   }

   public void sendTransaction(TransactionRequest tr) throws JMSException
   {
      sendBlocking(new SendTransactionMessage(tr));
   }


   public void setClientID(String clientID) throws JMSException
   {
      ConnectionState currentState = getState();

      if (currentState.getClientID() != null)
      {
         throw new javax.jms.IllegalStateException("Client id has already been set");
      }
      if (!currentState.isJustCreated())
      {
         throw new IllegalStateException("setClientID can only be called directly after the connection is created");
      }

      currentState.setClientID(clientID);

      currentState.setJustCreated(false);

      // this gets invoked on the server too
      invokeSetClientID(clientID);
      
   }
   private void invokeSetClientID(String clientID) throws JMSException
   {
      sendBlocking(new SetClientIDMessage(clientID));
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setExceptionListener(ExceptionListener listener) throws JMSException
   {
      state.setJustCreated(false);

      state.getRemotingConnection().getConnectionListener().
         addJMSExceptionListener(listener);
   }

   public void start() throws JMSException
   {
      state.setStarted(true);
      state.setJustCreated(false);
      sendOneWay(new StartConnectionMessage());
   }
   
   public void startAfterFailover() throws JMSException
   {
      sendOneWay(new StartConnectionMessage());
   }

   public void stop() throws JMSException
   {
      state.setStarted(false);
      state.setJustCreated(false);
      sendBlocking(new StopConnectionMessage());
   }

   public MessagingXid[] getPreparedTransactions() throws JMSException
   {
      GetPreparedTransactionsResponse response = (GetPreparedTransactionsResponse) sendBlocking(new GetPreparedTransactionsRequest());
      
      return response.getXids();
   }

   /**
    * This invocation should be handled by the client-side interceptor chain.
    */
   public void registerFailoverListener(FailoverListener listener)
   {
      state.getFailoverCommandCenter().registerFailoverListener(listener);
   }

   /**
    * This invocation should be handled by the client-side interceptor chain.
    */
   public boolean unregisterFailoverListener(FailoverListener listener)
   {
      return state.getFailoverCommandCenter().unregisterFailoverListener(listener);
   }   

   // Public ---------------------------------------------------------------------------------------

   public void setRemotingConnection(JMSRemotingConnection conn)
   {
      this.remotingConnection = conn;
   }

   public JMSRemotingConnection getRemotingConnection()
   {
      return remotingConnection;
   }

   public int getServerID()
   {
      return serverID;
   }

   public Version getVersionToUse()
   {
      return versionToUse;
   }

   public void setVersionToUse(Version versionToUse)
   {
      this.versionToUse = versionToUse;
   }

   public String toString()
   {
      return "ConnectionDelegate[" + System.identityHashCode(this) + ", ID=" + id +
         ", SID=" + serverID + "]";
   }

   // Protected ------------------------------------------------------------------------------------

   // Streamable implementation -------------------------------------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);

      serverID = in.readInt();
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);

      out.writeInt(serverID);
   }

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner Classes --------------------------------------------------------------------------------

}
