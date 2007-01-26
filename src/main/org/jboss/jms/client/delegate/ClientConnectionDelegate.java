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

import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.transaction.xa.Xid;

import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.jms.client.FailoverListener;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.Version;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.remoting.Client;

/**
 * The client-side Connection delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientConnectionDelegate extends DelegateSupport implements ConnectionDelegate
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 6680015509555859038L;

   // Attributes -----------------------------------------------------------------------------------

   private int serverID;
   private transient JMSRemotingConnection remotingConnection;
   private Version versionToUse;
   
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionDelegate(int objectID, int serverID)
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

      // start the connection again on the serverEndpoint if necessary
      if (thisState.isStarted())
      {
         this.start();
      }
   }

   // ConnectionDelegate implementation ------------------------------------------------------------

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void close() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void closing() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public boolean isClosed()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    * @see org.jboss.jms.client.container.AsfAspect#handleCreateConnectionConsumer(org.jboss.aop.joinpoint.Invocation)
    */
   public JBossConnectionConsumer createConnectionConsumer(Destination dest,
                                                           String subscriptionName,
                                                           String messageSelector,
                                                           ServerSessionPool sessionPool,
                                                           int maxMessages) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    * @see org.jboss.jms.server.endpoint.advised.ConnectionAdvised#createSessionDelegate(boolean, int, boolean)
    */
   public SessionDelegate createSessionDelegate(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public String getClientID() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
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

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void sendTransaction(TransactionRequest request) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setClientID(String id) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setExceptionListener(ExceptionListener listener) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void start() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void stop() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public Xid[] getPreparedTransactions()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
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

   // Public ---------------------------------------------------------------------------------------

   public void init()
   {
      super.init();
   }

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
      return "ConnectionDelegate[" + id + ", SID=" + serverID + "]";
   }

   // Protected ------------------------------------------------------------------------------------

   protected Client getClient()
   {
      return ((ConnectionState)state).getRemotingConnection().getRemotingClient();
   }

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner Classes --------------------------------------------------------------------------------

}
