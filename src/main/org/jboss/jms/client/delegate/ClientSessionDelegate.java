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

import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.client.state.SessionState;
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
import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryRequest;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryResponse;
import org.jboss.messaging.core.remoting.wireformat.AddTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveryMessage;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationResponse;
import org.jboss.messaging.core.remoting.wireformat.DeleteTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.RecoverDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.SendMessage;
import org.jboss.messaging.core.remoting.wireformat.UnsubscribeMessage;

/**
 * The client-side Session delegate class.
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
public class ClientSessionDelegate extends DelegateSupport implements SessionDelegate
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientSessionDelegate.class);

   private static final long serialVersionUID = -8096852898620279131L;

   // Attributes -----------------------------------------------------------------------------------

   private int dupsOKBatchSize;
   
   private boolean strictTck;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientSessionDelegate(String objectID, int dupsOKBatchSize)
   {
      super(objectID);

      this.dupsOKBatchSize = dupsOKBatchSize;
   }
   
   public ClientSessionDelegate(String objectID, int dupsOKBatchSize, boolean strictTCK)
   {
      super(objectID);

      this.dupsOKBatchSize = dupsOKBatchSize;
      this.strictTck = strictTCK;
   }

   public ClientSessionDelegate()
   {
   }

   // DelegateSupport overrides --------------------------------------------------------------------

   public void synchronizeWith(DelegateSupport nd) throws Exception
   {
      log.trace(this + " synchronizing with " + nd);

      super.synchronizeWith(nd);

      DelegateSupport newDelegate = (DelegateSupport)nd;

      // synchronize server endpoint state

      // synchronize (recursively) the client-side state

      state.synchronizeWith(newDelegate.getState());
      
      JMSRemotingConnection conn = ((ConnectionState)state.getParent()).getRemotingConnection();
      
      client = conn.getRemotingClient();
      
      strictTck = conn.isStrictTck();
   }

   public void setState(HierarchicalState state)
   {
      super.setState(state);
      
      JMSRemotingConnection conn = ((ConnectionState)state.getParent()).getRemotingConnection();
      
      client = conn.getRemotingClient();
      
      strictTck = conn.isStrictTck();
   }

   // Closeable implementation ---------------------------------------------------------------------

   public void close() throws JMSException
   {
      sendBlocking(new CloseMessage());
   }

   public long closing(long sequence) throws JMSException
   {   	   
      long seq = ((SessionState)state).getNPSendSequence();
      ClosingRequest request = new ClosingRequest(seq);
      ClosingResponse response = (ClosingResponse) sendBlocking(request);
      return response.getID();
   }

   // SessionDelegate implementation ---------------------------------------------------------------

   public boolean acknowledgeDelivery(Ack ack) throws JMSException
   {
      AcknowledgeDeliveryRequest request = new AcknowledgeDeliveryRequest(ack.getDeliveryID());
         AcknowledgeDeliveryResponse  response = (AcknowledgeDeliveryResponse) sendBlocking(request);
         return response.isAcknowledged();
   }

   public void acknowledgeDeliveries(List acks) throws JMSException
   {
      sendBlocking(new AcknowledgeDeliveriesMessage(acks));
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
      sendBlocking(new AddTemporaryDestinationMessage(destination));
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
      CreateBrowserRequest request = new CreateBrowserRequest(queue, messageSelector);
      CreateBrowserResponse response = (CreateBrowserResponse) sendBlocking(request);
      return new ClientBrowserDelegate(response.getBrowserID());      
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
      CreateConsumerRequest request = new CreateConsumerRequest(destination, selector, noLocal, subscriptionName, connectionConsumer, started);
      CreateConsumerResponse response = (CreateConsumerResponse) sendBlocking(request);

      ClientConsumerDelegate delegate = new ClientConsumerDelegate(response.getConsumerID(), response.getBufferSize(), response.getMaxDeliveries(), response.getRedeliveryDelay());
      return delegate;
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
      CreateDestinationRequest request = new CreateDestinationRequest(queueName, true);      
      CreateDestinationResponse response = (CreateDestinationResponse) sendBlocking(request);
      return (JBossQueue) response.getDestination();
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
      CreateDestinationRequest request = new CreateDestinationRequest(topicName, false);      
      CreateDestinationResponse response = (CreateDestinationResponse) sendBlocking(request);
      return (JBossTopic) response.getDestination();
   }

   public void deleteTemporaryDestination(JBossDestination destination) throws JMSException
   {
      sendBlocking(new DeleteTemporaryDestinationMessage(destination));
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
   public boolean postDeliver() throws JMSException
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
      sendBlocking(new UnsubscribeMessage(subscriptionName));
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
   
   public void send(JBossMessage m, boolean checkForDuplicates) throws JMSException
   {   	
   	long seq;
   	
   	if (m.isReliable() || strictTck)
   	{
   		seq = -1;
   	}
   	else
   	{
   		SessionState sstate = (SessionState)state;
   		
   		seq = sstate.getNPSendSequence();
   		
   		sstate.incNpSendSequence();
   	}
   	
   	SendMessage message = new SendMessage(m, checkForDuplicates, seq);
   	sendBlocking(message);
   	
   	
//      RequestSupport req = new SessionSendRequest(id, version, m, checkForDuplicates, seq);
//
//      if (seq == -1)
//      {      
//      	doInvoke(client, req);
//      }
//      else
//      {
//      	doInvokeOneway(client, req);
//      }
   }

   public void cancelDeliveries(List cancels) throws JMSException
   {
      sendBlocking(new CancelDeliveriesMessage(cancels));
   }

   public void cancelDelivery(Cancel cancel) throws JMSException
   {
      sendBlocking(new CancelDeliveryMessage(cancel));
   }

   public void recoverDeliveries(List deliveries, String sessionID) throws JMSException
   {
      sendBlocking(new RecoverDeliveriesMessage(deliveries, sessionID));
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

   public boolean isStrictTck()
   {
      return strictTck;
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
