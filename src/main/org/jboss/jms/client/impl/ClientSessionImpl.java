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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.TransactionInProgressException;
import javax.transaction.xa.XAResource;

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
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.wireformat.AddTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingMessage;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationResponse;
import org.jboss.messaging.core.remoting.wireformat.DeleteTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionSendMessage;
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
   
   private boolean xa;

   private int lazyAckBatchSize;
   
   private XAResource xaResource;
           
   private volatile boolean closed;
      
   private boolean acked = true;
   
   private boolean broken;
   
   private long toAckCount;
   
   private long lastID = -1;
   
   private long deliverID;      
   
   private boolean deliveryExpired;   

   // Executor used for executing onMessage methods
   private ClearableQueuedExecutor executor;

   private MessagingRemotingConnection remotingConnection;
         
   private ClientConnection connection;
   
   private Set<ClientBrowser> browsers = new HashSet<ClientBrowser>();
   
   private Set<ClientProducer> producers = new HashSet<ClientProducer>();
   
   private Map<String, ClientConsumer> consumers = new HashMap<String, ClientConsumer>();
   
      
   // Constructors ---------------------------------------------------------------------------------
   
   public ClientSessionImpl(ClientConnection connection, String id,
                            int lazyAckBatchSize, boolean xa)
   {
      this.id = id;
      
      this.connection = connection;
      
      this.remotingConnection = connection.getRemotingConnection();
      
      this.xa = xa;
 
      executor = new ClearableQueuedExecutor(new LinkedQueue());
      
      this.lazyAckBatchSize = lazyAckBatchSize;   
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
   
         executor.shutdownNow();
      }
      finally
      {
         connection.removeChild(id);
         
         closed = true;
      }
   }
  
   public void closing() throws JMSException
   {
      if (closed)
      {
         return;
      }
      
      closeChildren();
      
      //Make sure any remaining acks make it to the server
      
      acknowledgeInternal(false);      
                 
      ClosingMessage request = new ClosingMessage();
      
      remotingConnection.sendBlocking(id, request);
   }

   public ClientConnection getConnection()
   {
      return connection;
   }

   public void addTemporaryDestination(Destination destination) throws JMSException
   {
      checkClosed();
      
      remotingConnection.sendBlocking(id, new AddTemporaryDestinationMessage(destination));
   }

   public void commit() throws JMSException
   {
      checkClosed();
        
      if (isXA())
      {
         throw new TransactionInProgressException("Cannot call commit on an XA session");
      }

      //Before committing we must make sure the acks make it to the server
      //instead of this we could possibly add the lastDeliveryID in the SessionCommitMessage
      acknowledgeInternal(false);
      
      remotingConnection.sendBlocking(id, new SessionCommitMessage());
   }
   
   public void rollback() throws JMSException
   {
      checkClosed();
            
      if (isXA())
      {
         throw new TransactionInProgressException("Cannot call rollback on an XA session");
      }
      
      //First we tell each consumer to clear it's buffers and ignore any deliveries with
      //delivery id > last delivery id
      
      for (ClientConsumer consumer: consumers.values())
      {
         consumer.recover(lastID + 1);
      }
      
      //Before rolling back we must make sure the acks make it to the server
      //instead of this we could possibly add the lastDeliveryID in the SessionRollbackMessage
      acknowledgeInternal(false);      

      remotingConnection.sendBlocking(id, new SessionRollbackMessage());
   }

   public ClientBrowser createClientBrowser(Destination queue, String messageSelector)
      throws JMSException
   {
      checkClosed();
      
      String coreSelector = SelectorTranslator.convertToJBMFilterString(messageSelector);
      
      CreateBrowserRequest request = new CreateBrowserRequest(queue, coreSelector);
      
      CreateBrowserResponse response = (CreateBrowserResponse)remotingConnection.sendBlocking(id, request);
      
      ClientBrowser browser = new ClientBrowserImpl(remotingConnection, this, response.getBrowserID());  
      
      browsers.add(browser);
      
      return browser;
   }
   
   public ClientConsumer createClientConsumer(Destination destination, String selector,
                                              boolean noLocal, String subscriptionName) throws JMSException
   {
      checkClosed();
      
      String coreSelector = SelectorTranslator.convertToJBMFilterString(selector);
      
      CreateConsumerRequest request =
         new CreateConsumerRequest(destination, coreSelector, noLocal, subscriptionName, false);
      
      CreateConsumerResponse response = (CreateConsumerResponse)remotingConnection.sendBlocking(id, request);
      
      ClientConsumer consumer =
         new ClientConsumerImpl(this, response.getConsumerID(), response.getBufferSize(),             
                                destination,
                                selector, noLocal,
                                executor, remotingConnection);

      consumers.put(response.getConsumerID(), consumer);

      PacketDispatcher.client.register(new ClientConsumerPacketHandler(consumer, response.getConsumerID()));

      //Now we have finished creating the client consumer, we can tell the SCD
      //we are ready
      consumer.changeRate(1);
      
      return consumer;
   }
   
   public ClientProducer createClientProducer(JBossDestination destination) throws JMSException
   {
      checkClosed();
      
      ClientProducer producer = new ClientProducerImpl(this, destination);
  
      producers.add(producer);
      
      return producer;
   }

   public JBossQueue createQueue(String queueName) throws JMSException
   {
      checkClosed();
      
      CreateDestinationRequest request = new CreateDestinationRequest(queueName, true);  
      
      CreateDestinationResponse response = (CreateDestinationResponse)remotingConnection.sendBlocking(id, request);
      
      return (JBossQueue) response.getDestination();
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
   
   //Internal method to be called from consumerImpl - should not expose this publicly
   public void delivered(long deliverID, boolean expired)
   {
      this.deliverID = deliverID;
      
      this.deliveryExpired = expired;
   }
   
   //Called after a message has been delivered
   public void delivered() throws JMSException
   {                        
      if (lastID + 1 != deliverID)
      {
         broken = true;
      }
            
      lastID = deliverID;
            
      toAckCount++;
      
      acked = false;
       
      if (deliveryExpired)
      {
         remotingConnection.sendOneWay(id, new SessionCancelMessage(lastID, true));
         
         toAckCount = 0;
      }
      else if (broken)
      {
         //Must always ack now
         acknowledgeInternal(false);
         
         toAckCount = 0;
      }
      else
      {
         if (toAckCount == lazyAckBatchSize)
         {
            acknowledgeInternal(false);
            
            toAckCount = 0;
         }                       
      }            
   }
   
   private void acknowledgeInternal(boolean block) throws JMSException
   {
      if (acked)
      {
         return;
      }
      
      if (broken)
      {
         if (block)
         {
            remotingConnection.sendBlocking(id, new SessionAcknowledgeMessage(lastID, false));
         }
         else
         {
            remotingConnection.sendOneWay(id, new SessionAcknowledgeMessage(lastID, false));
         }
      }
      else
      {
         if (block)
         {
            remotingConnection.sendBlocking(id, new SessionAcknowledgeMessage(lastID, true));
         }
         else
         {
            remotingConnection.sendOneWay(id, new SessionAcknowledgeMessage(lastID, true));
         }
      }
      
      acked = true;
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

   public void send(Message m) throws JMSException
   {
      checkClosed();
      
      SessionSendMessage message = new SessionSendMessage(m);
      
      if (m.isDurable())
      {
         remotingConnection.sendBlocking(id, message);
      }
      else
      {
         remotingConnection.sendOneWay(id, message);
      }
   }
   
   public void removeConsumer(ClientConsumer consumer) throws JMSException
   {
      consumers.remove(consumer.getID());
            
      //1. flush any unacked message to the server
      
      acknowledgeInternal(false);
      
      //2. cancel all deliveries on server but not in tx
            
      remotingConnection.sendBlocking(id, new SessionCancelMessage(-1, false));      
   }
   
   public void removeProducer(ClientProducer producer)
   {
      producers.remove(producer);
   }
   
   public void removeBrowser(ClientBrowser browser)
   {
      browsers.remove(browser);
   }
     
   public boolean isXA() throws JMSException
   {
      checkClosed();
      
      return xa;
   }
   
//   public boolean isTransacted() throws JMSException
//   {
//      checkClosed();
//      
//      return transacted;
//   }
   
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
        
   private void closeChildren() throws JMSException
   {

      Set<ClientConsumer> consumersClone = new HashSet<ClientConsumer>(consumers.values());
      
      for (ClientConsumer consumer: consumersClone)
      {
         consumer.closing();
         
         consumer.close();
      }
      
      Set<ClientProducer> producersClone = new HashSet<ClientProducer>(producers);
      
      for (ClientProducer producer: producersClone)
      {
         producer.closing();
         
         producer.close();
      }
      
      Set<ClientBrowser> browsersClone = new HashSet<ClientBrowser>(browsers);
      
      for (ClientBrowser browser: browsersClone)
      {
         browser.closing();
         
         browser.close();
      }
   }
   
   // Inner Classes --------------------------------------------------------------------------------

}
