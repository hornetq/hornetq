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
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

import org.jboss.jms.client.FailoverListener;
import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.IDBlock;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.tx.MessagingXid;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.wireformat.CloseRequest;
import org.jboss.jms.wireformat.ClosingRequest;
import org.jboss.jms.wireformat.ConnectionCreateSessionDelegateRequest;
import org.jboss.jms.wireformat.ConnectionGetClientIDRequest;
import org.jboss.jms.wireformat.ConnectionGetIDBlockRequest;
import org.jboss.jms.wireformat.ConnectionGetPreparedTransactionsRequest;
import org.jboss.jms.wireformat.ConnectionSendTransactionRequest;
import org.jboss.jms.wireformat.ConnectionSetClientIDRequest;
import org.jboss.jms.wireformat.ConnectionStartRequest;
import org.jboss.jms.wireformat.ConnectionStopRequest;
import org.jboss.jms.wireformat.RequestSupport;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Version;

/**
 * The client-side Connection delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientConnectionDelegate extends DelegateSupport implements ConnectionDelegate
{
   // Constants ------------------------------------------------------------------------------------

	private static final long serialVersionUID = -5485083713058725777L;

	private static final Logger log = Logger.getLogger(ClientConnectionDelegate.class);

   // Attributes -----------------------------------------------------------------------------------

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
      log.debug(this + " synchronizing with " + nd);

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


      // The remoting connection was replaced by a new one..
      // we have to set the connection Delegate on the CallbackManager to avoid leaks
      remotingConnection.getCallbackManager().setConnectionDelegate(this);

      client = thisState.getRemotingConnection().getRemotingClient();

      serverID = newDelegate.getServerID();
   }

   public void setState(HierarchicalState state)
   {
      super.setState(state);

      client = ((ConnectionState)state).getRemotingConnection().getRemotingClient();
   }

   // Closeable implementation ---------------------------------------------------------------------

   public void close() throws JMSException
   {
      RequestSupport req = new CloseRequest(id, version);

      doInvoke(client, req);
   }

   public long closing() throws JMSException
   {
      RequestSupport req = new ClosingRequest(id, version);

      return ((Long)doInvoke(client, req)).longValue();
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
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public SessionDelegate createSessionDelegate(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA) throws JMSException
   {
      RequestSupport req =
         new ConnectionCreateSessionDelegateRequest(id, version, transacted,
                                                    acknowledgmentMode, isXA);

      return (SessionDelegate)doInvoke(client, req);
   }


   public String getClientID() throws JMSException
   {
      RequestSupport req = new ConnectionGetClientIDRequest(id, version);

      return (String)doInvoke(client, req);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ConnectionMetaData getConnectionMetaData() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ExceptionListener getExceptionListener() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public void sendTransaction(TransactionRequest request,
                               boolean checkForDuplicates) throws JMSException
   {
      RequestSupport req =
         new ConnectionSendTransactionRequest(id, version, request, checkForDuplicates);

      doInvoke(client, req);
   }

   public void setClientID(String clientID) throws JMSException
   {
      RequestSupport req = new ConnectionSetClientIDRequest(id, version, clientID);

      doInvoke(client, req);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setExceptionListener(ExceptionListener listener) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public void start() throws JMSException
   {
      RequestSupport req = new ConnectionStartRequest(id, version);

      doInvoke(client, req);
   }

   public void stop() throws JMSException
   {
      RequestSupport req = new ConnectionStopRequest(id, version);

      doInvoke(client, req);
   }

   public MessagingXid[] getPreparedTransactions() throws JMSException
   {
      RequestSupport req = new ConnectionGetPreparedTransactionsRequest(id, version);

      return (MessagingXid[])doInvoke(client, req);
   }

   /**
    * This invocation should be handled by the client-side interceptor chain.
    */
   public void registerFailoverListener(FailoverListener l)
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should be handled by the client-side interceptor chain.
    */
   public boolean unregisterFailoverListener(FailoverListener l)
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   public IDBlock getIdBlock(int size) throws JMSException
   {
      RequestSupport req = new ConnectionGetIDBlockRequest(id, version, size);

      return (IDBlock)doInvoke(client, req);
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
