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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TransactionInProgressException;
import javax.transaction.xa.XAResource;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.client.SelectorTranslator;
import org.jboss.jms.client.api.ClientBrowser;
import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.api.ClientConsumer;
import org.jboss.jms.client.api.ClientProducer;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
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
import org.jboss.messaging.util.Logger;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision: 3603 $</tt>
 *
 * $Id: ClientSessionImpl.java 3603 2008-01-21 18:49:20Z timfox $
 */
public class ClientSessionImpl implements ClientSession
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientSessionImpl.class);

   private boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private String id;
   
   private int dupsOKBatchSize;
   
   private boolean strictTck;
   
   private ClientConnection connection;
   
   private Map<String, Closeable> children = new ConcurrentHashMap<String, Closeable>();
   
   private int acknowledgeMode;
   
   private boolean transacted;
   
   private boolean xa;

   private MessagingXAResource xaResource;
   
   private Object currentTxId;

   // Executor used for executing onMessage methods
   private ClearableQueuedExecutor executor;

   private boolean recoverCalled;
   
   private List<Ack> clientAckList = new ArrayList<Ack>();

   private DeliveryInfo autoAckInfo;

   //This is somewhat strange - but some of the MQ and TCK tests expect an XA session to behavior as AUTO_ACKNOWLEDGE when not enlisted in
   //a transaction
   //This is the opposite behavior as what is required when the XA session handles MDB delivery or when using the message bridge.
   //In that case we want it to act as transacted, so when the session is subsequently enlisted the work can be converted into the
   //XA transaction
   private boolean treatAsNonTransactedWhenNotEnlisted = true;
   
   private long npSendSequence;
   
   private MessagingRemotingConnection remotingConnection;
   
   private volatile boolean closed;
   
   // Constructors ---------------------------------------------------------------------------------
   
   public ClientSessionImpl(ClientConnection connection, String id, int dupsOKBatchSize, boolean strictTCK,
         boolean transacted, int acknowledgmentMode, boolean xa)
   {
      this.id = id;
      this.connection = connection;
      this.remotingConnection = connection.getRemotingConnection();
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
   }
   
   // ClientSession implementation ----------------------------------------------------
   
   public String getID()
   {
      return id;
   }

   public synchronized void close() throws JMSException
   {
      if (closed)
      {
         return;
      }

      try
      {
         remotingConnection.sendBlocking(id, new CloseMessage());
   
         Object xid = getCurrentTxId();
   
         if (xid != null)
         {
            //Remove transaction from the resource manager
            connection.getResourceManager().removeTx(xid);
         }
   
         // We must explicitly shutdown the executor
   
         executor.shutdownNow();
      }
      finally
      {
         connection.removeChild(id);
         
         closed = true;
      }
   }
  
   public long closing(long sequence) throws JMSException
   {
      if (closed)
      {
         return -1;
      }
      
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

         if (autoAckInfo != null)
         {
            if (trace) { log.trace(this + " handleClosing(). Found remaining auto ack. Will ack " + autoAckInfo); }

            try
            {
               ackDelivery(autoAckInfo);

               if (trace) { log.trace(this + " acked it"); }
            }
            finally
            {
               autoAckInfo = null;
            }
         }
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         //Ack any remaining deliveries

         if (!clientAckList.isEmpty())
         {
            try
            {
               acknowledgeDeliveries(clientAckList);
            }
            finally
            {
               clientAckList.clear();

               autoAckInfo = null;
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

         internalCancelDeliveries(clientAckList);

         clientAckList.clear();
      }
      else if (isTransacted() && !isXA())
      {
         //We need to explicitly cancel any deliveries back to the server
         //from the resource manager, otherwise delivery count won't be updated

         List dels = connection.getResourceManager().getDeliveriesForSession(this.getID());

         internalCancelDeliveries(dels);
      }

      ClosingRequest request = new ClosingRequest(npSendSequence);
      
      ClosingResponse response = (ClosingResponse)remotingConnection.sendBlocking(id, request);
      
      return response.getID();
   }

   public ClientConnection getConnection()
   {
      return connection;
   }

   public boolean acknowledgeDelivery(Ack ack) throws JMSException
   {
      checkClosed();
      
      AcknowledgeDeliveryRequest request = new AcknowledgeDeliveryRequest(ack.getDeliveryID());
      
      AcknowledgeDeliveryResponse  response =
            (AcknowledgeDeliveryResponse)remotingConnection.sendBlocking(id, request);
         
      return response.isAcknowledged();
   }

   public void acknowledgeDeliveries(List<Ack> acks) throws JMSException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(id, new AcknowledgeDeliveriesMessage(acks));
   }

   public void acknowledgeAll() throws JMSException
   {
      checkClosed();
      
      if (!clientAckList.isEmpty())
      {
         //CLIENT_ACKNOWLEDGE can't be used with a MDB so it is safe to always acknowledge all
         //on this session (rather than the connection consumer session)
         acknowledgeDeliveries(clientAckList);

         clientAckList.clear();
      }
   }

   public void addTemporaryDestination(Destination destination) throws JMSException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(id, new AddTemporaryDestinationMessage(destination));
   }

   public void commit() throws JMSException
   {
      checkClosed();
      
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

   public ClientBrowser createClientBrowser(Destination queue, String messageSelector)
      throws JMSException
   {
      checkClosed();
      
      String coreSelector = SelectorTranslator.convertToJBMFilterString(messageSelector);
      
      CreateBrowserRequest request = new CreateBrowserRequest(queue, coreSelector);
      
      CreateBrowserResponse response = (CreateBrowserResponse)remotingConnection.sendBlocking(id, request);
      
      ClientBrowser browser = new ClientBrowserImpl(remotingConnection, this, response.getBrowserID());  
      
      children.put(response.getBrowserID(), browser);
      
      return browser;
   }
   
   public ClientConsumer createClientConsumer(Destination destination, String selector,
                                                  boolean noLocal, String subscriptionName,
                                                  boolean isCC) throws JMSException
   {
      checkClosed();
      
      String coreSelector = SelectorTranslator.convertToJBMFilterString(selector);
      
      CreateConsumerRequest request =
         new CreateConsumerRequest(destination, coreSelector, noLocal, subscriptionName, isCC);
      
      CreateConsumerResponse response = (CreateConsumerResponse)remotingConnection.sendBlocking(id, request);
      
      ClientConsumer consumer =
         new ClientConsumerImpl(this, response.getConsumerID(), response.getBufferSize(),
               response.getMaxDeliveries(), response.getRedeliveryDelay(),
            destination,
            selector, noLocal,
            isCC, executor, remotingConnection);

      children.put(response.getConsumerID(), consumer);

      PacketDispatcher.client.register(new ClientConsumerPacketHandler(consumer, response.getConsumerID()));

      //Now we have finished creating the client consumer, we can tell the SCD
      //we are ready
      consumer.changeRate(1);
      
      return consumer;
   }
   
   public JBossBytesMessage createBytesMessage() throws JMSException
   {
      checkClosed();
      
      JBossBytesMessage jbm = new JBossBytesMessage();
      
      return jbm;
   }

   public JBossMapMessage createMapMessage() throws JMSException
   {
      checkClosed();
      
      JBossMapMessage jbm = new JBossMapMessage();
      
      return jbm;
   }

   public JBossMessage createMessage() throws JMSException
   {
      checkClosed();
      
      JBossMessage jbm = new JBossMessage();
      
      return jbm;
   }

   public JBossObjectMessage createObjectMessage() throws JMSException
   {
      checkClosed();
   
      JBossObjectMessage jbm = new JBossObjectMessage();
      
      return jbm;
   }

   public JBossObjectMessage createObjectMessage(Serializable object) throws JMSException
   {
      checkClosed();
      
      JBossObjectMessage jbm = new JBossObjectMessage();
      
      jbm.setObject(object);
      
      return jbm;
   }

   public ClientProducer createClientProducer(JBossDestination destination) throws JMSException
   {
      checkClosed();
      
      ClientProducer producer = new ClientProducerImpl(this, destination);
  
      children.put(producer.getID(), producer);
      
      return producer;
   }

   public JBossQueue createQueue(String queueName) throws JMSException
   {
      checkClosed();
      
      CreateDestinationRequest request = new CreateDestinationRequest(queueName, true);  
      
      CreateDestinationResponse response = (CreateDestinationResponse)remotingConnection.sendBlocking(id, request);
      
      return (JBossQueue) response.getDestination();
   }
   
   public JBossStreamMessage createStreamMessage() throws JMSException
   {
      checkClosed();
      
      JBossStreamMessage jbm = new JBossStreamMessage();
      
      return jbm;
   }

   public JBossTextMessage createTextMessage() throws JMSException
   {
      checkClosed();
      
      JBossTextMessage jbm = new JBossTextMessage();
      
      return jbm;
   }

   public JBossTextMessage createTextMessage(String text) throws JMSException
   {
      checkClosed();
      
      JBossTextMessage jbm = new JBossTextMessage();
      
      jbm.setText(text);
      
      return jbm;
   }

   public JBossTopic createTopic(String topicName) throws JMSException
   {
      checkClosed();
      
      CreateDestinationRequest request = new CreateDestinationRequest(topicName, false); 
      
      CreateDestinationResponse response = (CreateDestinationResponse)remotingConnection.sendBlocking(id, request);
      
      return (JBossTopic) response.getDestination();
   }

   public void deleteTemporaryDestination(Destination destination) throws JMSException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(id, new DeleteTemporaryDestinationMessage(destination));
   }

   public boolean postDeliver() throws JMSException
   {
      checkClosed();
      
      int ackMode = getAcknowledgeMode();

      boolean res = true;

      // if XA and there is no transaction enlisted on XA we will act as AutoAcknowledge
      // However if it's a MDB (if there is a DistinguishedListener) we should behaved as transacted
      if (ackMode == Session.AUTO_ACKNOWLEDGE || isXAAndConsideredNonTransacted())
      {
         // It is possible that session.recover() is called inside a message listener onMessage
         // method - i.e. between the invocations of preDeliver and postDeliver. In this case we
         // don't want to acknowledge the last delivered messages - since it will be redelivered.
         if (!recoverCalled)
         {
            if (autoAckInfo == null)
            {
               throw new IllegalStateException("Cannot find delivery to AUTO_ACKNOWLEDGE");
            }

            if (trace) { log.trace(this + " auto acknowledging delivery " + autoAckInfo); }

            // We clear the state in a finally so then we don't get a knock on
            // exception on the next ack since we haven't cleared the state. See
            // http://jira.jboss.org/jira/browse/JBMESSAGING-852

            //This is ok since the message is acked after delivery, then the client
            //could get duplicates anyway

            try
            {
               res = ackDelivery(autoAckInfo);
            }
            finally
            {
               autoAckInfo = null;
            }
         }
         else
         {
            if (trace) { log.trace(this + " recover called, so NOT acknowledging"); }

            recoverCalled = false;
         }
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         if (!recoverCalled)
         {
            if (clientAckList.size() >= getDupsOKBatchSize())
            {
               // We clear the state in a finally
               // http://jira.jboss.org/jira/browse/JBMESSAGING-852

               try
               {
                  acknowledgeDeliveries(clientAckList);
               }
               finally
               {
                  clientAckList.clear();
                  
                  autoAckInfo = null;
               }
            }
         }
         else
         {
            if (trace) { log.trace(this + " recover called, so NOT acknowledging"); }

            recoverCalled = false;
         }
         
         autoAckInfo = null;
      }

      return Boolean.valueOf(res);
   }

   public void preDeliver(DeliveryInfo info) throws JMSException
   {
      checkClosed();
      
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

         clientAckList.add(info);
      }
      // if XA and there is no transaction enlisted on XA we will act as AutoAcknowledge
      // However if it's a MDB (if there is a DistinguishedListener) we should behaved as transacted
      else if (ackMode == Session.AUTO_ACKNOWLEDGE || isXAAndConsideredNonTransacted())
      {
         // We collect the single acknowledgement in the state.

         if (trace) { log.trace(this + " added " + info + " to session state"); }

         autoAckInfo = info;
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         if (trace) { log.trace(this + " added to DUPS_OK_ACKNOWLEDGE list delivery " + info); }

         clientAckList.add(info);

         //Also set here - this would be used for recovery in a message listener
         autoAckInfo = info;
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


            ClientSession connectionConsumerSession =
               info.getConnectionConsumerSession();

            String sessionId = connectionConsumerSession != null ?
               connectionConsumerSession.getID() : this.getID();

            connection.getResourceManager().addAck(txID, sessionId, info);
         }
      }
   }

   public void recover() throws JMSException
   {
      checkClosed();
      
      if (isTransacted() && !isXAAndConsideredNonTransacted())
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }

      if (trace) { log.trace("recovering the session"); }

      int ackMode = getAcknowledgeMode();

      if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         List<Ack> dels = clientAckList;

         clientAckList = new ArrayList<Ack>();

         redeliver(dels);

         recoverCalled = true;
      }
      else if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE || isXAAndConsideredNonTransacted())
      { 
         //Don't recover if it's already to cancel

         if (autoAckInfo != null)
         {
            List<Ack> redels = new ArrayList<Ack>();

            redels.add(autoAckInfo);

            redeliver(redels);

            autoAckInfo = null;

            recoverCalled = true;
         }
      }
   }

   public void redeliver(List toRedeliver) throws JMSException
   {
      checkClosed();
      
      // We put the messages back in the front of their appropriate consumer buffers

      if (trace) { log.trace(this + " handleRedeliver() called: " + toRedeliver); }

      // Need to be redelivered in reverse order.
      for (int i = toRedeliver.size() - 1; i >= 0; i--)
      {
         DeliveryInfo info = (DeliveryInfo)toRedeliver.get(i);
         JBossMessage msg = info.getMessage();

         ClientConsumer handler = (ClientConsumer)children.get(info.getConsumerId());

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
   
   public void rollback() throws JMSException
   {
      checkClosed();
      
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

   public void unsubscribe(String subscriptionName) throws JMSException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(id, new UnsubscribeMessage(subscriptionName));
   }

   public XAResource getXAResource()
   {
      return xaResource;
   }

   public int getAcknowledgeMode() throws JMSException
   {
      checkClosed();
      
      return acknowledgeMode;
   }

   public void send(Message m) throws JMSException
   {
      checkClosed();
      
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
   
   public void removeChild(String key) throws JMSException
   {
      checkClosed();
      
      children.remove(key);
   }
     
   public void cancelDeliveries(List<Cancel> cancels) throws JMSException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(id, new CancelDeliveriesMessage(cancels));
   }

   public void cancelDelivery(Cancel cancel) throws JMSException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(id, new CancelDeliveryMessage(cancel));
   }
   
   public int getDupsOKBatchSize() throws JMSException
   {
      checkClosed();
      
      return dupsOKBatchSize;
   }

   public boolean isStrictTck() throws JMSException
   {
      checkClosed();
      
      return strictTck;
   }
   
   public boolean isXA() throws JMSException
   {
      checkClosed();
      
      return xa;
   }
   
   public Object getCurrentTxId()
   {
      return currentTxId;
   }
   
   public void setCurrentTxId(Object currentTxId)
   {
      this.currentTxId = currentTxId;
   }

   public void setTreatAsNonTransactedWhenNotEnlisted(
         boolean treatAsNonTransactedWhenNotEnlisted)
   {
      this.treatAsNonTransactedWhenNotEnlisted = treatAsNonTransactedWhenNotEnlisted;
   }
   
   public boolean isTransacted() throws JMSException
   {
      checkClosed();
      
      return transacted;
   }
   
   public boolean isClosed()
   {
      return closed;
   }

   // Public ---------------------------------------------------------------------------------------
  
   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void checkClosed() throws IllegalStateException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
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
         seq = npSendSequence++;
      }
      
      SendMessage message = new SendMessage(m, seq);
      
      if (seq == -1)
      {
         remotingConnection.sendBlocking(id, message);
      }
      else
      {
         remotingConnection.sendOneWay(id, message);
      }
   }
   
   private void closeChildren() throws JMSException
   {
      Set<Closeable> chilrenValues = new HashSet<Closeable>(children.values());
      for (Closeable child: chilrenValues)
      {
         child.closing(-1);
         child.close();
      }
   }
   
   /** http://jira.jboss.org/jira/browse/JBMESSAGING-946 -
    *    To accomodate TCK and the MQ behavior
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
   private boolean isXAAndConsideredNonTransacted() throws JMSException
   {
      return isXA() && (getCurrentTxId() instanceof LocalTx)
             && treatAsNonTransactedWhenNotEnlisted;          
   }


   private boolean ackDelivery(DeliveryInfo delivery) throws JMSException
   {
   	ClientSession connectionConsumerSession = delivery.getConnectionConsumerSession();

      //If the delivery was obtained via a connection consumer we need to ack via that
      //otherwise we just use this session

      ClientSession sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : this;

      return sessionToUse.acknowledgeDelivery(delivery);
   }

   private void cancelDelivery(DeliveryInfo delivery) throws JMSException
   {
	   ClientSession connectionConsumerSession = delivery.getConnectionConsumerSession();

      //If the delivery was obtained via a connection consumer we need to cancel via that
      //otherwise we just use this session

	   ClientSession sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : this;

      sessionToUse.cancelDelivery(new CancelImpl(delivery.getDeliveryID(),
                                  delivery.getMessage().getDeliveryCount(), false, false));   	
   }

   private void internalCancelDeliveries( List deliveryInfos) throws JMSException
   {
      List cancels = new ArrayList();

      for (Iterator i = deliveryInfos.iterator(); i.hasNext(); )
      {
         DeliveryInfo ack = (DeliveryInfo)i.next();

         CancelImpl cancel = new CancelImpl(ack.getMessage().getDeliveryId(),
                                                  ack.getMessage().getDeliveryCount(),
                                                  false, false);

         cancels.add(cancel);         
      }

      if (!cancels.isEmpty())
      {
         this.cancelDeliveries(cancels);
      }
   }

   private void acknowledgeDeliveries(ClientSession del, List deliveryInfos) throws JMSException
   {
      if (!deliveryInfos.isEmpty())
      {
         del.acknowledgeDeliveries(deliveryInfos);
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
