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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TransactionInProgressException;
import javax.transaction.xa.XAResource;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.client.SelectorTranslator;
import org.jboss.jms.client.api.ClientBrowser;
import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.api.ClientProducer;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.api.Consumer;
import org.jboss.jms.client.container.ClientConsumer;
import org.jboss.jms.client.remoting.CallbackManager;
import org.jboss.jms.delegate.Ack;
import org.jboss.jms.delegate.Cancel;
import org.jboss.jms.delegate.DefaultCancel;
import org.jboss.jms.delegate.DeliveryInfo;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.jms.tx.LocalTx;
import org.jboss.jms.tx.MessagingXAResource;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.PacketDispatcher;
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
import org.jboss.messaging.core.remoting.wireformat.SendMessage;
import org.jboss.messaging.core.remoting.wireformat.UnsubscribeMessage;
import org.jboss.messaging.util.ClearableQueuedExecutor;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessageQueueNameHelper;
import org.jboss.messaging.util.ProxyFactory;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

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
public class ClientSessionDelegate extends CommunicationSupport<ClientSessionDelegate> implements ClientSession
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientSessionDelegate.class);

   private boolean trace = log.isTraceEnabled();

   private static final long serialVersionUID = -8096852898620279131L;

   // Attributes -----------------------------------------------------------------------------------

   private int dupsOKBatchSize;
   
   private boolean strictTck;
   
   private ClientConnection connection;
   
   // Attributes that used to live on SessionState -------------------------------------------------
   
   protected Set<Closeable> children = new ConcurrentHashSet<Closeable>();

   
   private int acknowledgeMode;
   private boolean transacted;
   private boolean xa;

   private MessagingXAResource xaResource;
   private Object currentTxId;

   // Executor used for executing onMessage methods
   private ClearableQueuedExecutor executor;

   private boolean recoverCalled;
   
   // List<DeliveryInfo>
   private List<Ack> clientAckList;

   private DeliveryInfo autoAckInfo;
   private Map callbackHandlers = new ConcurrentHashMap();
   
   private LinkedList asfMessages = new LinkedList();
   
   //The distinguished message listener - for ASF
   private MessageListener sessionListener;
   
   //This is somewhat strange - but some of the MQ and TCK tests expect an XA session to behavior as AUTO_ACKNOWLEDGE when not enlisted in
   //a transaction
   //This is the opposite behavior as what is required when the XA session handles MDB delivery or when using the message bridge.
   //In that case we want it to act as transacted, so when the session is subsequently enlisted the work can be converted into the
   //XA transaction
   private boolean treatAsNonTransactedWhenNotEnlisted = true;
   
   private long npSendSequence;
   
   // Constructors ---------------------------------------------------------------------------------
   

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientSessionDelegate(ClientConnection connection, String objectID, int dupsOKBatchSize)
   {
      super(objectID);
      this.connection = connection;
      this.dupsOKBatchSize = dupsOKBatchSize;
   }
   
   public ClientSessionDelegate(ClientConnectionDelegate connection, String objectID, int dupsOKBatchSize, boolean strictTCK,
         boolean transacted, int acknowledgmentMode, boolean xa)
   {
      super(objectID);

      this.connection = connection;
      this.dupsOKBatchSize = dupsOKBatchSize;
      this.strictTck = strictTCK;
      this.transacted = transacted;
      this.xa = xa;
      this.acknowledgeMode = acknowledgmentMode;
      executor = new ClearableQueuedExecutor(new LinkedQueue());
      
      if (xa)
      {
         // Create an XA resource
         xaResource = new MessagingXAResource(connection.getResourceManager(), this);
      }

      
      if (transacted)
      {
         // Create a local tx
         currentTxId = connection.getResourceManager().createLocalTx();
      }
      
      clientAckList = new ArrayList();
   }

   public ClientSessionDelegate()
   {
   }

   // DelegateSupport overrides --------------------------------------------------------------------

   // Closeable implementation ---------------------------------------------------------------------

   public void close() throws JMSException
   {
      sendBlocking(new CloseMessage());

      Object xid = getCurrentTxId();

      if (xid != null)
      {
         //Remove transaction from the resource manager
         connection.getResourceManager().removeTx(xid);
      }

      // We must explicitly shutdown the executor

      getExecutor().shutdownNow();

   }

   private long invokeClosing(long sequence) throws JMSException
   {   	   
      long seq = getNPSendSequence();
      ClosingRequest request = new ClosingRequest(seq);
      ClosingResponse response = (ClosingResponse) sendBlocking(request);
      return response.getID();
   }
   
   private void closeChildren() throws JMSException
   {
      for (Closeable child: children)
      {
         child.closing(-1);
         child.close();
      }
   }

   public long closing(long sequence) throws JMSException
   {
      if (trace) { log.trace("handleClosing()"); }

      closeChildren();
      
      //Sanity check
      if (isXA() && !isXAAndConsideredNonTransacted())
      {
         if (trace) { log.trace("Session is XA"); }

         ResourceManager rm = connection.getResourceManager();

         // An XASession should never be closed if there is prepared ack work that has not yet been
         // committed or rolled back. Imagine if messages had been consumed in the session, and
         // prepared but not committed. Then the connection was explicitly closed causing the
         // session to close. Closing the session causes any outstanding delivered but unacked
         // messages to be cancelled to the server which means they would be available for other
         // consumers to consume. If another consumer then consumes them, then recover() is called
         // and the original transaction is committed, then this means the same message has been
         // delivered twice which breaks the once and only once delivery guarantee.

         if (rm.checkForAcksInSession(this.getID()))
         {
            throw new javax.jms.IllegalStateException(
               "Attempt to close an XASession when there are still uncommitted acknowledgements!");
         }
      }

      int ackMode = getAcknowledgeMode();

      //We need to either ack (for auto_ack) or cancel (for client_ack)
      //any deliveries - this is because the message listener might have closed
      //before on message had finished executing

      if (ackMode == Session.AUTO_ACKNOWLEDGE || isXAAndConsideredNonTransacted())
      {
         //Acknowledge or cancel any outstanding auto ack

         DeliveryInfo remainingAutoAck = getAutoAckInfo();

         if (remainingAutoAck != null)
         {
            if (trace) { log.trace(this + " handleClosing(). Found remaining auto ack. Will ack " + remainingAutoAck); }

            try
            {
               ackDelivery(remainingAutoAck);

               if (trace) { log.trace(this + " acked it"); }
            }
            finally
            {
               setAutoAckInfo(null);
            }
         }
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         //Ack any remaining deliveries

         if (!getClientAckList().isEmpty())
         {
            try
            {
               acknowledgeDeliveries(getClientAckList());
            }
            finally
            {
               getClientAckList().clear();

               setAutoAckInfo(null);
            }
         }
      }
      else if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         // Cancel any oustanding deliveries
         // We cancel any client ack or transactional, we do this explicitly so we can pass the
         // updated delivery count information from client to server. We could just do this on the
         // server but we would lose delivery count info.

         // CLIENT_ACKNOWLEDGE cannot be used with MDBs (i.e. no connection consumer)
         // so is always safe to cancel on this session

         internalCancelDeliveries(getClientAckList());

         getClientAckList().clear();
      }
      else if (isTransacted() && !isXA())
      {
         //We need to explicitly cancel any deliveries back to the server
         //from the resource manager, otherwise delivery count won't be updated

         List dels = connection.getResourceManager().getDeliveriesForSession(this.getID());

         internalCancelDeliveries(dels);
      }

      return invokeClosing(sequence);

   }

   // SessionDelegate implementation ---------------------------------------------------------------

   public ClientConnection getConnection()
   {
      return connection;
   }

   public void setConnection(ClientConnection connection)
   {
      this.connection = connection;
   }

   
   public boolean acknowledgeDelivery(Ack ack) throws JMSException
   {
      AcknowledgeDeliveryRequest request = new AcknowledgeDeliveryRequest(ack.getDeliveryID());
         AcknowledgeDeliveryResponse  response = (AcknowledgeDeliveryResponse) sendBlocking(request);
         
      return response.isAcknowledged();
   }

   public void acknowledgeDeliveries(List<Ack> acks) throws JMSException
   {
      sendBlocking(new AcknowledgeDeliveriesMessage(acks));
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void acknowledgeAll() throws JMSException
   {
      if (!getClientAckList().isEmpty())
      {
         //CLIENT_ACKNOWLEDGE can't be used with a MDB so it is safe to always acknowledge all
         //on this session (rather than the connection consumer session)
         acknowledgeDeliveries(getClientAckList());

         getClientAckList().clear();
      }
   }

   public void addTemporaryDestination(Destination destination) throws JMSException
   {
      sendBlocking(new AddTemporaryDestinationMessage(destination));
   }

   public void commit() throws JMSException
   {
      if (!isTransacted())
      {
         throw new IllegalStateException("Cannot commit a non-transacted session");
      }

      if (isXA())
      {
         throw new TransactionInProgressException("Cannot call commit on an XA session");
      }

      try
      {
         connection.getResourceManager().commitLocal((LocalTx)getCurrentTxId(), connection);
      }
      finally
      {
         //Start new local tx
         setCurrentTxId( connection.getResourceManager().createLocalTx() );
      }

   }


   public ClientBrowser createBrowserDelegate(Destination queue, String messageSelector)
      throws JMSException
   {
      String coreSelector = SelectorTranslator.convertToJBMFilterString(messageSelector);
      CreateBrowserRequest request = new CreateBrowserRequest(queue, coreSelector);
      CreateBrowserResponse response = (CreateBrowserResponse) sendBlocking(request);
      ClientBrowserDelegate delegate = new ClientBrowserDelegate(this, response.getBrowserID(), queue, messageSelector);
      ClientBrowser proxy = (ClientBrowser)ProxyFactory.proxy(delegate, ClientBrowser.class);
      children.add(proxy);
      return proxy;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public JBossBytesMessage createBytesMessage() throws JMSException
   {
      JBossBytesMessage jbm = new JBossBytesMessage();
      return jbm;
   }


   public Consumer createConsumerDelegate(Destination destination, String selector,
                                                  boolean noLocal, String subscriptionName,
                                                  boolean isCC) throws JMSException
   {
      String coreSelector = SelectorTranslator.convertToJBMFilterString(selector);
      CreateConsumerRequest request =
         new CreateConsumerRequest(destination, coreSelector, noLocal, subscriptionName, isCC);
      
      CreateConsumerResponse response = (CreateConsumerResponse) sendBlocking(request);

      ClientConsumerDelegate consumerDelegate = new ClientConsumerDelegate(this, response.getConsumerID(), response.getBufferSize(), response.getMaxDeliveries(), response.getRedeliveryDelay(),
            destination,
            selector, noLocal, subscriptionName, response.getConsumerID(),isCC);      

      Consumer proxy = (Consumer)ProxyFactory.proxy(consumerDelegate, Consumer.class);
      
      children.add(proxy);

      //We need the queue name for recovering any deliveries after failover
      String queueName = null;
      if (subscriptionName != null)
      {
         // I have to use the clientID from connectionDelegate instead of connectionState...
         // this is because when a pre configured CF is used we need to get the clientID from
         // server side.
         // This was a condition verified by the TCK and it was fixed as part of
         // http://jira.jboss.com/jira/browse/JBMESSAGING-939
         queueName = MessageQueueNameHelper.
            createSubscriptionName(this.getID(),subscriptionName);
      }
      else if (destination.getType() == DestinationType.QUEUE)
      {
         queueName = destination.getName();
      }

      final ClientConsumer messageHandler =
         new ClientConsumer(isCC, this.getAcknowledgeMode(),
                            this, consumerDelegate, consumerDelegate.getID(), queueName,
                            consumerDelegate.getBufferSize(), this.getExecutor(), consumerDelegate.getMaxDeliveries(), consumerDelegate.isShouldAck(),
                            consumerDelegate.getRedeliveryDelay());

      this.addCallbackHandler(messageHandler);

      PacketDispatcher.client.register(new ClientConsumerPacketHandler(messageHandler, consumerDelegate.getID()));

      CallbackManager cm = connection.getRemotingConnection().getCallbackManager();
      cm.registerHandler(consumerDelegate.getID(), messageHandler);

      consumerDelegate.setClientConsumer(messageHandler);

      //Now we have finished creating the client consumer, we can tell the SCD
      //we are ready
      consumerDelegate.changeRate(1);
      
      return proxy;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public JBossMapMessage createMapMessage() throws JMSException
   {
      JBossMapMessage jbm = new JBossMapMessage();
      return jbm;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public JBossMessage createMessage() throws JMSException
   {
      JBossMessage jbm = new JBossMessage();
      return jbm;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public JBossObjectMessage createObjectMessage() throws JMSException
   {
      JBossObjectMessage jbm = new JBossObjectMessage();
      return jbm;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public JBossObjectMessage createObjectMessage(Serializable object) throws JMSException
   {
      JBossObjectMessage jbm = new JBossObjectMessage();
      jbm.setObject(object);
      return jbm;
   }


   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ClientProducer createProducerDelegate(JBossDestination destination) throws JMSException
   {
      // ProducerDelegates are not created on the server

      ClientProducerDelegate producerDelegate = new ClientProducerDelegate(connection, this, destination );
      ClientProducer proxy = (ClientProducer) ProxyFactory.proxy(producerDelegate, ClientProducer.class);
      children.add(proxy);
      return proxy;
   }

   public JBossQueue createQueue(String queueName) throws JMSException
   {
      CreateDestinationRequest request = new CreateDestinationRequest(queueName, true);      
      CreateDestinationResponse response = (CreateDestinationResponse) sendBlocking(request);
      return (JBossQueue) response.getDestination();
   }
   
   public JBossStreamMessage createStreamMessage() throws JMSException
   {
      JBossStreamMessage jbm = new JBossStreamMessage();
      return jbm;
   }

   public JBossTextMessage createTextMessage() throws JMSException
   {
      JBossTextMessage jbm = new JBossTextMessage();
      return jbm;
   }

   public JBossTextMessage createTextMessage(String text) throws JMSException
   {
      JBossTextMessage jbm = new JBossTextMessage();
      jbm.setText(text);
      return jbm;
   }

   public JBossTopic createTopic(String topicName) throws JMSException
   {
      CreateDestinationRequest request = new CreateDestinationRequest(topicName, false);      
      CreateDestinationResponse response = (CreateDestinationResponse) sendBlocking(request);
      return (JBossTopic) response.getDestination();
   }

   public void deleteTemporaryDestination(Destination destination) throws JMSException
   {
      sendBlocking(new DeleteTemporaryDestinationMessage(destination));
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public boolean postDeliver() throws JMSException
   {
      int ackMode = getAcknowledgeMode();

      boolean res = true;

      // if XA and there is no transaction enlisted on XA we will act as AutoAcknowledge
      // However if it's a MDB (if there is a DistinguishedListener) we should behaved as transacted
      if (ackMode == Session.AUTO_ACKNOWLEDGE || isXAAndConsideredNonTransacted())
      {
         // It is possible that session.recover() is called inside a message listener onMessage
         // method - i.e. between the invocations of preDeliver and postDeliver. In this case we
         // don't want to acknowledge the last delivered messages - since it will be redelivered.
         if (!isRecoverCalled())
         {
            DeliveryInfo delivery = getAutoAckInfo();

            if (delivery == null)
            {
               throw new IllegalStateException("Cannot find delivery to AUTO_ACKNOWLEDGE");
            }

            if (trace) { log.trace(this + " auto acknowledging delivery " + delivery); }

            // We clear the state in a finally so then we don't get a knock on
            // exception on the next ack since we haven't cleared the state. See
            // http://jira.jboss.org/jira/browse/JBMESSAGING-852

            //This is ok since the message is acked after delivery, then the client
            //could get duplicates anyway

            try
            {
               res = ackDelivery(delivery);
            }
            finally
            {
               setAutoAckInfo(null);
            }
         }
         else
         {
            if (trace) { log.trace(this + " recover called, so NOT acknowledging"); }

            setRecoverCalled(false);
         }
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         List acks = getClientAckList();

         if (!isRecoverCalled())
         {
            if (acks.size() >= getDupsOKBatchSize())
            {
               // We clear the state in a finally
               // http://jira.jboss.org/jira/browse/JBMESSAGING-852

               try
               {
                  acknowledgeDeliveries(acks);
               }
               finally
               {
                  acks.clear();
                  setAutoAckInfo(null);
               }
            }
         }
         else
         {
            if (trace) { log.trace(this + " recover called, so NOT acknowledging"); }

            setRecoverCalled(false);
         }
         setAutoAckInfo(null);
      }

      return Boolean.valueOf(res);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void preDeliver(DeliveryInfo info) throws JMSException
   {
      int ackMode = getAcknowledgeMode();

      if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         // We collect acknowledgments in the list

         if (trace) { log.trace(this + " added to CLIENT_ACKNOWLEDGE list delivery " + info); }

         // Sanity check
         if (info.getConnectionConsumerSession() != null)
         {
            throw new IllegalStateException(
               "CLIENT_ACKNOWLEDGE cannot be used with a connection consumer");
         }

         getClientAckList().add(info);
      }
      // if XA and there is no transaction enlisted on XA we will act as AutoAcknowledge
      // However if it's a MDB (if there is a DistinguishedListener) we should behaved as transacted
      else if (ackMode == Session.AUTO_ACKNOWLEDGE || isXAAndConsideredNonTransacted())
      {
         // We collect the single acknowledgement in the state.

         if (trace) { log.trace(this + " added " + info + " to session state"); }

         setAutoAckInfo(info);
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         if (trace) { log.trace(this + " added to DUPS_OK_ACKNOWLEDGE list delivery " + info); }

         getClientAckList().add(info);

         //Also set here - this would be used for recovery in a message listener
         setAutoAckInfo(info);
      }
      else
      {
         Object txID = getCurrentTxId();

         if (txID != null)
         {
            // the session is non-XA and transacted, or XA and enrolled in a global transaction. An
            // XA session that has not been enrolled in a global transaction behaves as a
            // transacted session.

            if (trace) { log.trace("sending acknowlegment transactionally, queueing on resource manager"); }

            // If the ack is for a delivery that came through via a connection consumer then we use
            // the connectionConsumer session as the session id, otherwise we use this sessions'
            // session ID


            ClientSession connectionConsumerDelegate =
               info.getConnectionConsumerSession();

            String sessionId = connectionConsumerDelegate != null ?
               connectionConsumerDelegate.getID() : this.getID();

            connection.getResourceManager().addAck(txID, sessionId, info);
         }
      }
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void recover() throws JMSException
   {
      if (trace) { log.trace("recover called"); }

      if (isTransacted() && !isXAAndConsideredNonTransacted())
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }

      if (trace) { log.trace("recovering the session"); }

      int ackMode = getAcknowledgeMode();

      if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         List dels = getClientAckList();

         setClientAckList(new ArrayList());

         redeliver(dels);

         setRecoverCalled(true);
      }
      else if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE || isXAAndConsideredNonTransacted())
      {
         DeliveryInfo info = getAutoAckInfo();

         //Don't recover if it's already to cancel

         if (info != null)
         {
            List redels = new ArrayList();

            redels.add(info);

            redeliver(redels);

            setAutoAckInfo(null);

            setRecoverCalled(true);
         }
      }
   }

   /**
    * Redelivery occurs in two situations:
    *
    * 1) When session.recover() is called (JMS1.1 4.4.11)
    *
    * "A session's recover method is used to stop a session and restart it with its first
    * unacknowledged message. In effect, the session's series of delivered messages is reset to the
    * point after its last acknowledged message."
    *
    * An important note here is that session recovery is LOCAL to the session. Session recovery DOES
    * NOT result in delivered messages being cancelled back to the channel where they can be
    * redelivered - since that may result in them being picked up by another session, which would
    * break the semantics of recovery as described in the spec.
    *
    * 2) When session rollback occurs (JMS1.1 4.4.7). On rollback of a session the spec is clear
    * that session recovery occurs:
    *
    * "If a transaction rollback is done, its produced messages are destroyed and its consumed
    * messages are automatically recovered. For more information on session recovery, see Section
    * 4.4.11 'Message Acknowledgment.'"
    *
    * So on rollback we do session recovery (local redelivery) in the same as if session.recover()
    * was called.
    *
    * All cancellation at rollback is driven from the client side - we always attempt to redeliver
    * messages to their original consumers if they are still open, or then cancel them to the server
    * if they are not. Cancelling them to the server explicitly allows the delivery count to be updated.
    *
    *
    */
   public void redeliver(List toRedeliver) throws JMSException
   {
      // We put the messages back in the front of their appropriate consumer buffers

      if (trace) { log.trace(this + " handleRedeliver() called: " + toRedeliver); }

      // Need to be redelivered in reverse order.
      for (int i = toRedeliver.size() - 1; i >= 0; i--)
      {
         DeliveryInfo info = (DeliveryInfo)toRedeliver.get(i);
         JBossMessage msg = info.getMessage();

         ClientConsumer handler = getCallbackHandler(info.getConsumerId());

         if (handler == null)
         {
            // This is ok. The original consumer has closed, so we cancel the message

            cancelDelivery(info);
         }
         else if (handler.getRedeliveryDelay() != 0)
         {
         	//We have a redelivery delay in action - all delayed redeliveries are handled on the server

         	cancelDelivery(info);
         }
         else
         {
            if (trace) { log.trace("Adding proxy back to front of buffer"); }

            handler.addToFrontOfBuffer(msg);
         }
      }

   }
   
   public ClientConsumer getCallbackHandler(String consumerID)
   {
      return (ClientConsumer)callbackHandlers.get(consumerID);
   }

   public void addCallbackHandler(ClientConsumer handler)
   {
      callbackHandlers.put(handler.getConsumerId(), handler);
   }

   public void removeCallbackHandler(ClientConsumer handler)
   {
      callbackHandlers.remove(handler.getConsumerId());
   }

   

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void rollback() throws JMSException
   {
      if (!isTransacted())
      {
         throw new IllegalStateException("Cannot rollback a non-transacted session");
      }

      if (isXA())
      {
         throw new TransactionInProgressException("Cannot call rollback on an XA session");
      }

      ResourceManager rm = connection.getResourceManager();
      try
      {
         rm.rollbackLocal(getCurrentTxId());
      }
      finally
      {
         // startnew local tx
         Object xid = rm.createLocalTx();
         setCurrentTxId(xid);
      }
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void run() throws JMSException
   {
      if (trace) { log.trace("run()"); }

      int ackMode = getAcknowledgeMode();

      LinkedList msgs = getAsfMessages();

      while (msgs.size() > 0)
      {
         AsfMessageHolder holder = (AsfMessageHolder)msgs.removeFirst();

         if (trace) { log.trace("sending " + holder.msg + " to the message listener" ); }

         ClientConsumer.callOnMessage(this, getDistinguishedListener(), holder.consumerID,
                                      false,
                                      holder.msg, ackMode, holder.maxDeliveries,
                                      holder.connectionConsumerDelegate, holder.shouldAck);
      }
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setMessageListener(MessageListener listener) throws JMSException
   {
      if (trace) { log.trace("setMessageListener()"); }

      if (listener == null)
      {
         throw new IllegalStateException("Cannot set a null MessageListener on the session");
      }

      setDistinguishedListener(listener);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public MessageListener getMessageListener() throws JMSException
   {
      if (trace) { log.trace("getMessageListener()"); }

      return getDistinguishedListener();
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
      return xaResource;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public int getAcknowledgeMode()
   {
      return acknowledgeMode;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void addAsfMessage(JBossMessage m, String theConsumerID, String queueName, int maxDeliveries,
                             ClientSession connectionConsumerDelegate, boolean shouldAck) throws JMSException
   {
      // Load the session with a message to be processed during a subsequent call to run()

      if (m == null)
      {
         throw new IllegalStateException("Cannot add a null message to the session");
      }

      AsfMessageHolder holder = new AsfMessageHolder();
      holder.msg = m;
      holder.consumerID = theConsumerID;
      holder.queueName = queueName;
      holder.maxDeliveries = maxDeliveries;
      holder.connectionConsumerDelegate = connectionConsumerDelegate;
      holder.shouldAck = shouldAck;

      getAsfMessages().add(holder);
   }

   public void send(Message m) throws JMSException
   {
      Object txID = getCurrentTxId();

      // If there is no GlobalTransaction we run it as local transacted
      // as discussed at http://www.jboss.com/index.html?module=bb&op=viewtopic&t=98577
      // http://jira.jboss.org/jira/browse/JBMESSAGING-946
      // and
      // http://jira.jboss.org/jira/browse/JBMESSAGING-410
      if ((!isXA() && isTransacted()) || (isXA() && !(txID instanceof LocalTx)))
      {
         // the session is non-XA and transacted, or XA and enrolled in a global transaction, so
         // we add the message to a transaction instead of sending it now. An XA session that has
         // not been enrolled in a global transaction behaves as a non-transacted session.

         if (trace) { log.trace("sending message " + m + " transactionally, queueing on resource manager txID=" + txID + " sessionID= " + getID()); }

         connection.getResourceManager().addMessage(txID, this.getID(), m);

         // ... and we don't invoke any further interceptors in the stack
         return;
      }

      if (trace) { log.trace("sending message NON-transactionally"); }

      invokeSend(m);

   }
   
   private void invokeSend(Message m) throws JMSException
   {   	
   	long seq;
   	
   	if (m.isDurable() || strictTck)
   	{
   		seq = -1;
   	}
   	else
   	{
   		seq = this.getNPSendSequence();
   		
   		this.incNpSendSequence();
   	}
   	
   	SendMessage message = new SendMessage(m, seq);
   	
   	if (seq == -1)
   	{
   	   sendBlocking(message);
   	}
   	else
   	{
   	   sendOneWay(message);
   	}
   }

   public void cancelDeliveries(List<Cancel> cancels) throws JMSException
   {
      sendBlocking(new CancelDeliveriesMessage(cancels));
   }

   public void cancelDelivery(Cancel cancel) throws JMSException
   {
      sendBlocking(new CancelDeliveryMessage(cancel));
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

   @Override
   protected Client getClient()
   {
      return connection.getClient();
   }
   
   @Override
   protected byte getVersion()
   {
      return connection.getVersion();
   }

   
   
   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   /** http://jira.jboss.org/jira/browse/JBMESSAGING-946 - To accomodate TCK and the MQ behavior
    *    we should behave as non transacted, AUTO_ACK when there is no transaction enlisted
    *    However when the Session is being used by ASF we should consider the case where
    *    we will convert LocalTX to GlobalTransactions.
    *    This function helper will ensure the condition that needs to be tested on this aspect
    *
    *    There is a real conundrum here:
    *
    *    An XA Session needs to act as transacted when not enlisted for consuming messages for an MDB so when it does
    *    get enlisted we can transfer the work inside the tx
    *
    *    But in needs to act as auto_acknowledge when not enlisted and not in an MDB (or bridge or stress test) to satisfy
    *    integration tests and TCK!!! Hence getTreatAsNonTransactedWhenNotEnlisted()
    *
    * */
   private boolean isXAAndConsideredNonTransacted()
   {
      return isXA() && (getCurrentTxId() instanceof LocalTx) && getTreatAsNonTransactedWhenNotEnlisted()
             && getDistinguishedListener() == null;
   }


   private boolean ackDelivery(DeliveryInfo delivery) throws JMSException
   {
   	if (delivery.isShouldAck())
   	{
	      ClientSession connectionConsumerSession = delivery.getConnectionConsumerSession();

	      //If the delivery was obtained via a connection consumer we need to ack via that
	      //otherwise we just use this session

	      ClientSession sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : this;

	      return sessionToUse.acknowledgeDelivery(delivery);
   	}
   	else
   	{
   		return true;
   	}
   }

   private void cancelDelivery(DeliveryInfo delivery) throws JMSException
   {
   	if (delivery.isShouldAck())
   	{
   	   ClientSession connectionConsumerSession = delivery.getConnectionConsumerSession();

	      //If the delivery was obtained via a connection consumer we need to cancel via that
	      //otherwise we just use this session

   	   ClientSession sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : this;

	      sessionToUse.cancelDelivery(new DefaultCancel(delivery.getDeliveryID(),
	                                  delivery.getMessage().getDeliveryCount(), false, false));
   	}
   }

   private void internalCancelDeliveries( List deliveryInfos) throws JMSException
   {
      List cancels = new ArrayList();

      for (Iterator i = deliveryInfos.iterator(); i.hasNext(); )
      {
         DeliveryInfo ack = (DeliveryInfo)i.next();

         if (ack.isShouldAck())
         {
	         DefaultCancel cancel = new DefaultCancel(ack.getMessage().getDeliveryId(),
	                                                  ack.getMessage().getDeliveryCount(),
	                                                  false, false);

	         cancels.add(cancel);
         }
      }

      if (!cancels.isEmpty())
      {
         this.cancelDeliveries(cancels);
      }
   }

   private void acknowledgeDeliveries(ClientSession del, List deliveryInfos) throws JMSException
   {
      List acks = new ArrayList();

      for (Iterator i = deliveryInfos.iterator(); i.hasNext(); )
      {
         DeliveryInfo ack = (DeliveryInfo)i.next();

         if (ack.isShouldAck())
         {
	         acks.add(ack);
         }
      }

      if (!acks.isEmpty())
      {
         del.acknowledgeDeliveries(acks);
      }
   }

   // Inner Classes --------------------------------------------------------------------------------


   private static class AsfMessageHolder
   {
      private JBossMessage msg;
      private String consumerID;
      private String queueName;
      private int maxDeliveries;
      private ClientSession connectionConsumerDelegate;
      private boolean shouldAck;
   }

   
   // TODO verify what should be exposed or not!
   public boolean isXA()
   {
      return xa;
   }

   public void setXA(boolean xa)
   {
      this.xa = xa;
   }

   public Object getCurrentTxId()
   {
      return currentTxId;
   }

   public void setCurrentTxId(Object currentTxId)
   {
      this.currentTxId = currentTxId;
   }

   public boolean isRecoverCalled()
   {
      return recoverCalled;
   }

   public void setRecoverCalled(boolean recoverCalled)
   {
      this.recoverCalled = recoverCalled;
   }

   public List<Ack> getClientAckList()
   {
      return clientAckList;
   }

   public void setClientAckList(List<Ack> clientAckList)
   {
      this.clientAckList = clientAckList;
   }

   public DeliveryInfo getAutoAckInfo()
   {
      return autoAckInfo;
   }

   public void setAutoAckInfo(DeliveryInfo autoAckInfo)
   {
      this.autoAckInfo = autoAckInfo;
   }

   public Map getCallbackHandlers()
   {
      return callbackHandlers;
   }

   public void setCallbackHandlers(Map callbackHandlers)
   {
      this.callbackHandlers = callbackHandlers;
   }

   public LinkedList getAsfMessages()
   {
      return asfMessages;
   }

   public void setAsfMessages(LinkedList asfMessages)
   {
      this.asfMessages = asfMessages;
   }

   public MessageListener getSessionListener()
   {
      return sessionListener;
   }

   public void setSessionListener(MessageListener sessionListener)
   {
      this.sessionListener = sessionListener;
   }

   public boolean isTreatAsNonTransactedWhenNotEnlisted()
   {
      return treatAsNonTransactedWhenNotEnlisted;
   }

   public void setTreatAsNonTransactedWhenNotEnlisted(
         boolean treatAsNonTransactedWhenNotEnlisted)
   {
      this.treatAsNonTransactedWhenNotEnlisted = treatAsNonTransactedWhenNotEnlisted;
   }

   public long getNpSendSequence()
   {
      return npSendSequence;
   }

   public void setNpSendSequence(long npSendSequence)
   {
      this.npSendSequence = npSendSequence;
   }

   public ClearableQueuedExecutor getExecutor()
   {
      return executor;
   }

   public void setDupsOKBatchSize(int dupsOKBatchSize)
   {
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   public void setStrictTck(boolean strictTck)
   {
      this.strictTck = strictTck;
   }

   public void setAcknowledgeMode(int acknowledgeMode)
   {
      this.acknowledgeMode = acknowledgeMode;
   }
   
   public boolean isTransacted()
   {
      return transacted;
   }

   

   
   public void setTransacted(boolean transacted)
   {
      this.transacted = transacted;
   }

   public long getNPSendSequence()
   {
      return npSendSequence;
   }
   
   public void incNpSendSequence()
   {
      npSendSequence++;
   }
   
   public boolean getTreatAsNonTransactedWhenNotEnlisted()
   {
      return treatAsNonTransactedWhenNotEnlisted;
   }
   

   public MessageListener getDistinguishedListener()
   {
      return this.sessionListener;
   }
   
   public void setDistinguishedListener(MessageListener listener)
   {
      this.sessionListener = listener;
   }
   

}
