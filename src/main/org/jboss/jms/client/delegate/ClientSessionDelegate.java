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
import java.io.Serializable;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.transaction.xa.XAResource;

import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.delegate.Ack;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.Cancel;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.DeliveryInfo;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.BytesMessageProxy;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MapMessageProxy;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.message.ObjectMessageProxy;
import org.jboss.jms.message.StreamMessageProxy;
import org.jboss.jms.message.TextMessageProxy;
import org.jboss.jms.wireformat.CloseRequest;
import org.jboss.jms.wireformat.ClosingRequest;
import org.jboss.jms.wireformat.RequestSupport;
import org.jboss.jms.wireformat.SessionAcknowledgeDeliveriesRequest;
import org.jboss.jms.wireformat.SessionAcknowledgeDeliveryRequest;
import org.jboss.jms.wireformat.SessionAddTemporaryDestinationRequest;
import org.jboss.jms.wireformat.SessionCancelDeliveriesRequest;
import org.jboss.jms.wireformat.SessionCancelDeliveryRequest;
import org.jboss.jms.wireformat.SessionCreateBrowserDelegateRequest;
import org.jboss.jms.wireformat.SessionCreateConsumerDelegateRequest;
import org.jboss.jms.wireformat.SessionCreateQueueRequest;
import org.jboss.jms.wireformat.SessionCreateTopicRequest;
import org.jboss.jms.wireformat.SessionDeleteTemporaryDestinationRequest;
import org.jboss.jms.wireformat.SessionRecoverDeliveriesRequest;
import org.jboss.jms.wireformat.SessionSendRequest;
import org.jboss.jms.wireformat.SessionUnsubscribeRequest;
import org.jboss.logging.Logger;

/**
 * The client-side Session delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientSessionDelegate extends DelegateSupport implements SessionDelegate
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientSessionDelegate.class);

   private static final long serialVersionUID = -8096852898620279131L;

   // Attributes -----------------------------------------------------------------------------------

   private int dupsOKBatchSize;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientSessionDelegate(String objectID, int dupsOKBatchSize)
   {
      super(objectID);

      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   public ClientSessionDelegate()
   {
   }

   // DelegateSupport overrides --------------------------------------------------------------------

   public void synchronizeWith(DelegateSupport nd) throws Exception
   {
      log.trace(this + " synchronizing with " + nd);

      super.synchronizeWith(nd);

      ClientSessionDelegate newDelegate = (ClientSessionDelegate)nd;

      // synchronize server endpoint state

      // synchronize (recursively) the client-side state

      state.synchronizeWith(newDelegate.getState());

      client = ((ConnectionState)state.getParent()).getRemotingConnection().
         getRemotingClient();
   }

   public void setState(HierarchicalState state)
   {
      super.setState(state);

      client = ((ConnectionState)state.getParent()).getRemotingConnection().
                  getRemotingClient();
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

   // SessionDelegate implementation ---------------------------------------------------------------

   public void acknowledgeDelivery(Ack ack) throws JMSException
   {
      RequestSupport req = new SessionAcknowledgeDeliveryRequest(id, version, ack);

      doInvoke(client, req);
   }

   public void acknowledgeDeliveries(List acks) throws JMSException
   {
      RequestSupport req = new SessionAcknowledgeDeliveriesRequest(id, version, acks);

      doInvoke(client, req);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void acknowledgeAll() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public void addTemporaryDestination(JBossDestination destination) throws JMSException
   {
      RequestSupport req = new SessionAddTemporaryDestinationRequest(id, version, destination);

      doInvoke(client, req);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void redeliver() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void commit() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public BrowserDelegate createBrowserDelegate(JBossDestination queue, String messageSelector)
      throws JMSException
   {
      RequestSupport req = new SessionCreateBrowserDelegateRequest(id, version, queue,
                                                                   messageSelector);

      Object res = doInvoke(client, req);

      return (BrowserDelegate)res;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public BytesMessageProxy createBytesMessage() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }


   public ConsumerDelegate createConsumerDelegate(JBossDestination destination, String selector,
                                                  boolean noLocal, String subscriptionName,
                                                  boolean connectionConsumer, boolean started) throws JMSException
   {
      RequestSupport req = new SessionCreateConsumerDelegateRequest(id, version, destination,
                                                                    selector, noLocal, subscriptionName,
                                                                    connectionConsumer, started);

      return (ConsumerDelegate)doInvoke(client, req);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public MapMessageProxy createMapMessage() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public MessageProxy createMessage() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ObjectMessageProxy createObjectMessage() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ObjectMessageProxy createObjectMessage(Serializable object) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ProducerDelegate createProducerDelegate(JBossDestination destination) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public JBossQueue createQueue(String queueName) throws JMSException
   {
      RequestSupport req = new SessionCreateQueueRequest(id, version, queueName);

      return (JBossQueue)doInvoke(client, req);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public StreamMessageProxy createStreamMessage() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public TextMessageProxy createTextMessage() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public TextMessageProxy createTextMessage(String text) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public JBossTopic createTopic(String topicName) throws JMSException
   {
      RequestSupport req = new SessionCreateTopicRequest(id, version, topicName);

      return (JBossTopic)doInvoke(client, req);
   }

   public void deleteTemporaryDestination(JBossDestination destination) throws JMSException
   {
      RequestSupport req = new SessionDeleteTemporaryDestinationRequest(id, version, destination);

      doInvoke(client, req);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public MessageListener getMessageListener() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void postDeliver() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void preDeliver(DeliveryInfo deliveryInfo) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void recover() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void redeliver(List ackInfos) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void rollback() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void run()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setMessageListener(MessageListener listener) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   public void unsubscribe(String subscriptionName) throws JMSException
   {
      RequestSupport req = new SessionUnsubscribeRequest(id, version, subscriptionName);

      doInvoke(client, req);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public XAResource getXAResource()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public int getAcknowledgeMode()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public boolean getTransacted()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void addAsfMessage(MessageProxy m, String consumerID, String queueName, int maxDeliveries,
                             SessionDelegate connectionConsumerSession, boolean shouldAck)
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   private long sequence;

   public void send(JBossMessage m, boolean checkForDuplicates) throws JMSException
   {   	
   	long seq = m.isReliable() ? -1 : sequence++;
   	
      RequestSupport req = new SessionSendRequest(id, version, m, checkForDuplicates, seq);

      if (seq == -1)
      {      
      	doInvoke(client, req);
      }
      else
      {
      	doInvokeOneway(client, req);
      }
   }

   public void cancelDeliveries(List cancels) throws JMSException
   {
      RequestSupport req = new SessionCancelDeliveriesRequest(id, version, cancels);

      doInvoke(client, req);
   }

   public void cancelDelivery(Cancel cancel) throws JMSException
   {
      RequestSupport req = new SessionCancelDeliveryRequest(id, version, cancel);

      doInvoke(client, req);
   }

   public void recoverDeliveries(List acks, String sessionID) throws JMSException
   {
      RequestSupport req = new SessionRecoverDeliveriesRequest(id, version, acks, sessionID);

      doInvoke(client, req);
   }

   // Streamable overrides -------------------------------------------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);

      dupsOKBatchSize = in.readInt();
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);

      out.writeInt(dupsOKBatchSize);
   }

   // Public ---------------------------------------------------------------------------------------

   public int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }

   public String toString()
   {
      return "SessionDelegate[" + System.identityHashCode(this) + ", ID=" + id + "]";
   }

   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner Classes --------------------------------------------------------------------------------

}
