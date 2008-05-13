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
package org.jboss.messaging.core.client.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.jboss.messaging.util.SimpleString;

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
public class ClientSessionImpl implements ClientSessionInternal
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientSessionImpl.class);

   private boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private final ClientConnectionInternal connection;
      
   private final long serverTargetID;
   
   private final int lazyAckBatchSize;
   
   private final boolean cacheProducers;
   
   private final int defaultConsumerWindowSize;   
   
   private final int defaultConsumerMaxRate;
   
   private final int defaultProducerWindowSize;
   
   private final int defaultProducerMaxRate;
     
   private final ExecutorService executor;
   
   private volatile boolean closed;
      
   private boolean acked = true;
   
   private boolean broken;
   
   private long toAckCount;
   
   private long lastID = -1;
   
   private long deliverID;      
   
   private boolean deliveryExpired;   

   private final RemotingConnection remotingConnection;         
   
   private final Set<ClientBrowser> browsers = new HashSet<ClientBrowser>();
   
   private final Set<ClientProducer> producers = new HashSet<ClientProducer>();
   
   private final Map<Long, ClientConsumerInternal> consumers = new HashMap<Long, ClientConsumerInternal>();
   
   private final Map<SimpleString, ClientProducerInternal> producerCache;
   
   //For testing only
   private boolean forceNotSameRM;
   
   private long lastCommittedID = -1;
   
   private final boolean autoCommitAcks;
   
   private final boolean autoCommitSends;
   
   private final boolean blockOnAcknowledge;
   
   // Constructors ---------------------------------------------------------------------------------
   
   public ClientSessionImpl(final ClientConnectionInternal connection, final long serverTargetID,
                            final int lazyAckBatchSize, final boolean cacheProducers,                            
                            final boolean autoCommitSends, final boolean autoCommitAcks,
                            final boolean blockOnAcknowledge,
                            final int defaultConsumerWindowSize,  
                            final int defaultConsumerMaxRate,
                            final int defaultProducerWindowSize,
                            final int defaultProducerMaxRate) throws MessagingException
   {
   	if (lazyAckBatchSize < -1 || lazyAckBatchSize == 0)
   	{
   		throw new IllegalArgumentException("Invalid lazyAckbatchSize, valid values are > 0 or -1 (infinite)");
   	}
   	
      this.serverTargetID = serverTargetID;
      
      this.connection = connection;
      
      this.remotingConnection = connection.getRemotingConnection();
      
      this.cacheProducers = cacheProducers;
      
      this.defaultConsumerWindowSize = defaultConsumerWindowSize;
      
      this.defaultConsumerMaxRate = defaultConsumerMaxRate;
      
      this.defaultProducerWindowSize = defaultProducerWindowSize;
      
      this.defaultProducerMaxRate = defaultProducerMaxRate;
      
      executor = Executors.newSingleThreadExecutor();
      
      this.lazyAckBatchSize = lazyAckBatchSize;
      
      if (cacheProducers)
      {
      	producerCache = new HashMap<SimpleString, ClientProducerInternal>();
      }
      else
      {
      	producerCache = null;
      }
      
      this.autoCommitAcks = autoCommitAcks;
      
      this.autoCommitSends = autoCommitSends;
      
      this.blockOnAcknowledge = blockOnAcknowledge;
   }
   
   // ClientSession implementation -----------------------------------------------------------------

   public void createQueue(final SimpleString address, final SimpleString queueName, final SimpleString filterString,
   		                  final boolean durable, final boolean temporary)
                           throws MessagingException
   {
      checkClosed();

      SessionCreateQueueMessage request = new SessionCreateQueueMessage(address, queueName, filterString, durable, temporary);

      remotingConnection.sendBlocking(serverTargetID, serverTargetID, request);
   }

   public void deleteQueue(final SimpleString queueName) throws MessagingException
   {
      checkClosed();

      remotingConnection.sendBlocking(serverTargetID, serverTargetID, new SessionDeleteQueueMessage(queueName));
   }
   
   public SessionQueueQueryResponseMessage queueQuery(final SimpleString queueName) throws MessagingException
   {
      checkClosed();
      
      SessionQueueQueryMessage request = new SessionQueueQueryMessage(queueName);
      
      SessionQueueQueryResponseMessage response = (SessionQueueQueryResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, request);
      
      return response;
   }
   
   public SessionBindingQueryResponseMessage bindingQuery(final SimpleString address) throws MessagingException
   {
      checkClosed();
      
      SessionBindingQueryMessage request = new SessionBindingQueryMessage(address);
      
      SessionBindingQueryResponseMessage response = (SessionBindingQueryResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, request);
      
      return response;
   }
   
   public void addDestination(final SimpleString address, final boolean temporary) throws MessagingException
   {
      checkClosed();
      
      SessionAddDestinationMessage request = new SessionAddDestinationMessage(address, temporary);
      
      remotingConnection.sendBlocking(serverTargetID, serverTargetID, request);
   }
   
   public void removeDestination(final SimpleString address, final boolean temporary) throws MessagingException
   {
      checkClosed();
      
      SessionRemoveDestinationMessage request = new SessionRemoveDestinationMessage(address, temporary);
      
      remotingConnection.sendBlocking(serverTargetID, serverTargetID, request);  
   }
   
   public ClientConsumer createConsumer(final SimpleString queueName) throws MessagingException
   {
      return createConsumer(queueName, null, false, false);
   }
   
   public ClientConsumer createConsumer(final SimpleString queueName, final SimpleString filterString, final boolean noLocal,
                                        final boolean autoDeleteQueue) throws MessagingException
   {
      checkClosed();
      
      long clientTargetID = remotingConnection.getPacketDispatcher().generateID();
    
      SessionCreateConsumerMessage request =
         new SessionCreateConsumerMessage(clientTargetID, queueName, filterString, noLocal, autoDeleteQueue,
         		                           defaultConsumerWindowSize, defaultConsumerMaxRate);
      
      SessionCreateConsumerResponseMessage response = (SessionCreateConsumerResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, request);
      
      int tokenBatchSize = response.getWindowSize() == -1 ? 0 : 1;
      
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(this, response.getConsumerTargetID(), clientTargetID, executor, remotingConnection, tokenBatchSize);

      consumers.put(response.getConsumerTargetID(), consumer);
      

      remotingConnection.getPacketDispatcher().register(new ClientConsumerPacketHandler(consumer, clientTargetID));
      
      //Now we send window size tokens to start the consumption
      //We even send it if windowSize == -1, since we need to start the consumer
      
      remotingConnection.sendOneWay(response.getConsumerTargetID(), serverTargetID, new ConsumerFlowTokenMessage(response.getWindowSize()));

      return consumer;
   }
   
   public ClientBrowser createBrowser(final SimpleString queueName, final SimpleString filterString) throws MessagingException
   {
      checkClosed();

      SessionCreateBrowserMessage request = new SessionCreateBrowserMessage(queueName, filterString);

      SessionCreateBrowserResponseMessage response = (SessionCreateBrowserResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, request);

      ClientBrowser browser = new ClientBrowserImpl(response.getBrowserTargetID(), this, remotingConnection);  

      browsers.add(browser);

      return browser;
   }

   public ClientProducer createProducer(final SimpleString address) throws MessagingException
   {
      return createProducer(address, defaultProducerWindowSize, defaultProducerMaxRate);
   }
      
   public ClientProducer createProducer(final SimpleString address, final int windowSize, final int maxRate) throws MessagingException
   {
      checkClosed();
      
      ClientProducerInternal producer = null;
      
      if (cacheProducers)
      {
      	producer = producerCache.remove(address);
      }

      if (producer == null)
      {
         long clientTargetID = remotingConnection.getPacketDispatcher().generateID();
         
      	SessionCreateProducerMessage request = new SessionCreateProducerMessage(clientTargetID, address, windowSize, maxRate);
      	
      	SessionCreateProducerResponseMessage response =
      		(SessionCreateProducerResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, request);
      	
      	//maxRate and windowSize can be overridden by the server
      	
      	producer = new ClientProducerImpl(this, response.getProducerTargetID(), clientTargetID, address,
      			                            remotingConnection, response.getWindowSize(),
      			                            response.getMaxRate());  
      	
      	remotingConnection.getPacketDispatcher().register(new ClientProducerPacketHandler(producer, clientTargetID));
      }

      producers.add(producer);

      return producer;
   }
   
   public ClientProducer createRateLimitedProducer(SimpleString address, int rate) throws MessagingException
   {
   	return createProducer(address, -1, rate);
   }
   
   public ClientProducer createProducerWithWindowSize(SimpleString address, int windowSize) throws MessagingException
   {
   	return createProducer(address, windowSize, 0);
   }
   
   public XAResource getXAResource()
   {
      return this;
   }
   
   public void commit() throws MessagingException
   {
      checkClosed();
        
      acknowledgeInternal(false);
      
      remotingConnection.sendBlocking(serverTargetID, serverTargetID, new EmptyPacket(EmptyPacket.SESS_COMMIT));
      
      lastCommittedID = lastID;
   }
   
   public void rollback() throws MessagingException
   {
      checkClosed();
                
      //We tell each consumer to clear it's buffers and ignore any deliveries with
      //delivery serverTargetID > last delivery serverTargetID, until it gets delivery serverTargetID = lastID again
      
      if (autoCommitAcks)
      {
      	lastCommittedID = lastID;
      }
      
      for (ClientConsumerInternal consumer: consumers.values())
      {
         consumer.recover(lastCommittedID + 1);
      }
      
      //We flush any remaining acks
      
      acknowledgeInternal(false);      

      toAckCount = 0;

      remotingConnection.sendBlocking(serverTargetID, serverTargetID, new EmptyPacket(EmptyPacket.SESS_ROLLBACK));   
   }
   
   public void acknowledge() throws MessagingException
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
         remotingConnection.sendOneWay(serverTargetID, serverTargetID, new SessionCancelMessage(lastID, true));
         
         toAckCount = 0;
      }
      else if (broken || toAckCount == lazyAckBatchSize)
      {
         acknowledgeInternal(blockOnAcknowledge);
         
         toAckCount = 0;
         
         if (autoCommitAcks)
         {
         	lastCommittedID = lastID;
         }
      }
   }

   public synchronized void close() throws MessagingException
   {
      if (closed)
      {
         return;
      }

      try
      {
         closeChildren();
         
         if (cacheProducers)
         {
         	for (ClientProducer producer: producerCache.values())
         	{
         		producer.close();
         	}
         	
         	producerCache.clear();
         }
         
         //Make sure any remaining acks make it to the server
         
         acknowledgeInternal(false);      
         
         remotingConnection.sendBlocking(serverTargetID, serverTargetID, new EmptyPacket(EmptyPacket.CLOSE));            
      }
      finally
      {
      	executor.shutdown();
      	
         connection.removeSession(this);
         
         closed = true;
      }
   }
  
   public boolean isClosed()
   {
      return closed;
   }
   
   public boolean isAutoCommitSends()
   {
   	return autoCommitSends;
   }
   
   public boolean isAutoCommitAcks()
   {
   	return autoCommitAcks;   	   	
   }
   
   public int getLazyAckBatchSize()
   {
   	return lazyAckBatchSize;
   }
   
   // ClientSessionInternal implementation ------------------------------------------------------------
   
   public long getServerTargetID()
   {
      return serverTargetID;
   }
   
   public ClientConnectionInternal getConnection()
   {
      return connection;
   }

   public void delivered(final long deliverID, final boolean expired)
   {
      this.deliverID = deliverID;
      
      this.deliveryExpired = expired;
   }
   
   public void removeConsumer(final ClientConsumerInternal consumer) throws MessagingException
   {
      consumers.remove(consumer.getClientTargetID());
            
      //1. flush any unacked message to the server
      
      acknowledgeInternal(false);

      //2. cancel all deliveries on server but not in tx
            
      remotingConnection.sendBlocking(serverTargetID, serverTargetID, new SessionCancelMessage(-1, false));
   }
   
   public void removeProducer(final ClientProducerInternal producer)
   {
      producers.remove(producer);
      
      if (cacheProducers && !producerCache.containsKey(producer.getAddress()))
      {
      	producerCache.put(producer.getAddress(), producer);
      }
   }
   
   public void removeBrowser(final ClientBrowser browser)
   {
      browsers.remove(browser);
   }
   
   
      
   // XAResource implementation --------------------------------------------------------------------
   
   public void commit(final Xid xid, final boolean onePhase) throws XAException
   {
      try
      { 
         SessionXACommitMessage packet = new SessionXACommitMessage(xid, onePhase);
                  
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, packet);
         
         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (MessagingException e)
      {
         log.error("Caught jmsexecptione ", e);
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public void end(final Xid xid, final int flags) throws XAException
   {
      try
      {
         Packet packet;
         
         if (flags == XAResource.TMSUSPEND)
         {
            packet = new EmptyPacket(EmptyPacket.SESS_XA_SUSPEND);                  
         }
         else if (flags == XAResource.TMSUCCESS)
         {
            packet = new SessionXAEndMessage(xid, false);
         }
         else if (flags == XAResource.TMFAIL)
         {
            packet = new SessionXAEndMessage(xid, true);
         }
         else
         {
            throw new XAException(XAException.XAER_INVAL);
         }
               
         //Need to flush any acks to server first
         acknowledgeInternal(false);
         
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, packet);
         
         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (MessagingException e)
      {
         log.error("Caught jmsexecptione ", e);
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public void forget(final Xid xid) throws XAException
   {
      try
      {                              
         //Need to flush any acks to server first
         acknowledgeInternal(false);
                  
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, new SessionXAForgetMessage(xid));
         
         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (MessagingException e)
      {
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public int getTransactionTimeout() throws XAException
   {
      try
      {                              
         SessionXAGetTimeoutResponseMessage response =
            (SessionXAGetTimeoutResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, new EmptyPacket(EmptyPacket.SESS_XA_GET_TIMEOUT));
         
         return response.getTimeoutSeconds();
      }
      catch (MessagingException e)
      {
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public boolean isSameRM(final XAResource xares) throws XAException
   {
      if (!(xares instanceof ClientSessionImpl))
      {
         return false;
      }
      
      if (forceNotSameRM)
      {
         return false;
      }
      
      ClientSessionImpl other = (ClientSessionImpl)xares;
      
      return connection.getRemotingConnection().getLocation()
                .equals(other.getConnection().getRemotingConnection().getLocation());
   }

   public int prepare(final Xid xid) throws XAException
   {
      try
      {
         SessionXAPrepareMessage packet = new SessionXAPrepareMessage(xid);
         
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, packet);
         
         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
         else
         {
            return response.getResponseCode();
         }
      }
      catch (MessagingException e)
      {
         log.error("Caught jmsexecptione ", e);
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public Xid[] recover(final int flag) throws XAException
   {
      try
      {
         SessionXAGetInDoubtXidsResponseMessage response = (SessionXAGetInDoubtXidsResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, new EmptyPacket(EmptyPacket.SESS_XA_INDOUBT_XIDS));
         
         List<Xid> xids = response.getXids();
         
         Xid[] xidArray = xids.toArray(new Xid[xids.size()]);
         
         return xidArray;
      }
      catch (MessagingException e)
      {
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public void rollback(final Xid xid) throws XAException
   {
      try
      {
         SessionXARollbackMessage packet = new SessionXARollbackMessage(xid);
         
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, packet);
         
         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (MessagingException e)
      {
         log.error("Caught jmsexecptione ", e);
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public boolean setTransactionTimeout(final int seconds) throws XAException
   {
      try
      {                              
         SessionXASetTimeoutResponseMessage response =
            (SessionXASetTimeoutResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, new SessionXASetTimeoutMessage(seconds));
         
         return response.isOK();
      }
      catch (MessagingException e)
      {
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public void start(final Xid xid, final int flags) throws XAException
   {
      try
      {
         Packet packet;
         
         if (flags == XAResource.TMJOIN)
         {
            //Need to flush any acks to server first
            acknowledgeInternal(false);
                        
            packet = new SessionXAJoinMessage(xid);                  
         }
         else if (flags == XAResource.TMRESUME)
         {
            //Need to flush any acks to server first
            acknowledgeInternal(false);
                        
            packet = new SessionXAResumeMessage(xid);
         }
         else if (flags == XAResource.TMNOFLAGS)
         {
            packet = new SessionXAStartMessage(xid);
         }
         else
         {
            throw new XAException(XAException.XAER_INVAL);
         }
                     
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.sendBlocking(serverTargetID, serverTargetID, packet);
         
         if (response.isError())
         {
            log.error("XA operation failed " + response.getMessage() +" code:" + response.getResponseCode());
            throw new XAException(response.getResponseCode());
         }
      }
      catch (MessagingException e)
      {
         log.error("Caught jmsexecptione ", e);
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   // Public ---------------------------------------------------------------------------------------
  
   public void setForceNotSameRM(final boolean force)
   {
      this.forceNotSameRM = force;
   }
   
   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void acknowledgeInternal(final boolean block) throws MessagingException
   {
      if (acked)
      {
         return;
      }
      
      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(lastID, !broken);
            
      if (block)
      {
         remotingConnection.sendBlocking(serverTargetID, serverTargetID, message);
      }
      else
      {
         remotingConnection.sendOneWay(serverTargetID, serverTargetID, message);
      }
            
      acked = true;
   }
   
   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Session is closed");
      }
   }
        
   private void closeChildren() throws MessagingException
   {
      Set<ClientConsumer> consumersClone = new HashSet<ClientConsumer>(consumers.values());
      
      for (ClientConsumer consumer: consumersClone)
      {
         consumer.close();
      }
      
      Set<ClientProducer> producersClone = new HashSet<ClientProducer>(producers);
      
      for (ClientProducer producer: producersClone)
      {
         producer.close();
      }
      
      Set<ClientBrowser> browsersClone = new HashSet<ClientBrowser>(browsers);
      
      for (ClientBrowser browser: browsersClone)
      {
         browser.close();
      }
   }
   
   // Inner Classes --------------------------------------------------------------------------------

}
