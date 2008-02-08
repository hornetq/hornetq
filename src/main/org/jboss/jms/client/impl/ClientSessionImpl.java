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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.client.SelectorTranslator;
import org.jboss.jms.client.api.ClientBrowser;
import org.jboss.jms.client.api.ClientConsumer;
import org.jboss.jms.client.api.ClientProducer;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionAddAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRemoveAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetInDoubtXidsMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetTimeoutMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAStartMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASuspendMessage;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

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

   private String id;
   
   private int lazyAckBatchSize;
   
   private volatile boolean closed;
      
   private boolean acked = true;
   
   private boolean broken;
   
   private long toAckCount;
   
   private long lastID = -1;
   
   private long deliverID;      
   
   private boolean deliveryExpired;   

   private ExecutorService executor;

   private MessagingRemotingConnection remotingConnection;
         
   private ClientConnectionInternal connection;
   
   private Set<ClientBrowser> browsers = new HashSet<ClientBrowser>();
   
   private Set<ClientProducer> producers = new HashSet<ClientProducer>();
   
   private Map<String, ClientConsumerInternal> consumers = new HashMap<String, ClientConsumerInternal>();
   
   //For testing only
   private boolean forceNotSameRM;
   
   // Constructors ---------------------------------------------------------------------------------
   
   public ClientSessionImpl(ClientConnectionInternal connection, String id,
                            int lazyAckBatchSize) throws MessagingException
   {
      this.id = id;
      
      this.connection = connection;
      
      this.remotingConnection = connection.getRemotingConnection();
      
      executor = Executors.newSingleThreadExecutor();
      
      this.lazyAckBatchSize = lazyAckBatchSize;   
   }
   
   // ClientSession implementation -----------------------------------------------------------------

   public void createQueue(String address, String queueName, String filterString, boolean durable, boolean temporary)
                           throws MessagingException
   {
      checkClosed();

      SessionCreateQueueMessage request = new SessionCreateQueueMessage(address, queueName, filterString, durable, temporary);

      remotingConnection.send(id, request);
   }

   public void deleteQueue(String queueName) throws MessagingException
   {
      checkClosed();

      remotingConnection.send(id, new SessionDeleteQueueMessage(queueName));
   }
   
   public SessionQueueQueryResponseMessage queueQuery(String queueName) throws MessagingException
   {
      checkClosed();
      
      SessionQueueQueryMessage request = new SessionQueueQueryMessage(queueName);
      
      SessionQueueQueryResponseMessage response = (SessionQueueQueryResponseMessage)remotingConnection.send(id, request);
      
      return response;
   }
   
   public SessionBindingQueryResponseMessage bindingQuery(String address) throws MessagingException
   {
      checkClosed();
      
      SessionBindingQueryMessage request = new SessionBindingQueryMessage(address);
      
      SessionBindingQueryResponseMessage response = (SessionBindingQueryResponseMessage)remotingConnection.send(id, request);
      
      return response;
   }
   
   public void addAddress(String address) throws MessagingException
   {
      checkClosed();
      
      SessionAddAddressMessage request = new SessionAddAddressMessage(address);
      
      remotingConnection.send(id, request);
   }
   
   public void removeAddress(String address) throws MessagingException
   {
      checkClosed();
      
      SessionRemoveAddressMessage request = new SessionRemoveAddressMessage(address);
      
      remotingConnection.send(id, request);  
   }
   
   public ClientConsumer createConsumer(String queueName, String filterString, boolean noLocal,
                                        boolean autoDeleteQueue, boolean direct) throws MessagingException
   {
      checkClosed();
    
      SessionCreateConsumerMessage request =
         new SessionCreateConsumerMessage(queueName, filterString, noLocal, autoDeleteQueue);
      
      SessionCreateConsumerResponseMessage response = (SessionCreateConsumerResponseMessage)remotingConnection.send(id, request);
      
      int prefetchSize = response.getPrefetchSize();
            
      ClientConsumerInternal consumer =
         new ClientConsumerImpl(this, response.getConsumerID(),             
                                executor, remotingConnection, direct, response.getPrefetchSize());

      consumers.put(response.getConsumerID(), consumer);

      PacketDispatcher.client.register(new ClientConsumerPacketHandler(consumer, response.getConsumerID()));

      if (prefetchSize > 0) // 0 ==> flow control is disabled
      {
         //Now give the server consumer some initial tokens (1.5 * prefetchSize)
         
         int initialTokens = prefetchSize + prefetchSize >>> 1;
         
         remotingConnection.send(response.getConsumerID(), new ConsumerFlowTokenMessage(initialTokens), true);
      }
      else
      {
         //FIXME
         //FIXME - for now we need to send a flow control token to ensure the return packet sender gets set
         //FIXME
         remotingConnection.send(response.getConsumerID(), new ConsumerFlowTokenMessage(1), true);
      }
      
      return consumer;
   }
   
   public ClientBrowser createBrowser(String queueName, String messageSelector) throws MessagingException
   {
      checkClosed();

      String coreSelector = SelectorTranslator.convertToJBMFilterString(messageSelector);

      SessionCreateBrowserMessage request = new SessionCreateBrowserMessage(queueName, coreSelector);

      SessionCreateBrowserResponseMessage response = (SessionCreateBrowserResponseMessage)remotingConnection.send(id, request);

      ClientBrowser browser = new ClientBrowserImpl(remotingConnection, this, response.getBrowserID());  

      browsers.add(browser);

      return browser;
   }

   public ClientProducer createProducer() throws MessagingException
   {
      checkClosed();

      ClientProducer producer = new ClientProducerImpl(this);

      producers.add(producer);

      return producer;
   }
   
   public XAResource getXAResource()
   {
      return this;
   }
   
   public void commit() throws MessagingException
   {
      checkClosed();
        
      acknowledgeInternal(false);
      
      remotingConnection.send(id, new SessionCommitMessage());
   }
   
   public void rollback() throws MessagingException
   {
      checkClosed();
            
      //First we tell each consumer to clear it's buffers and ignore any deliveries with
      //delivery id > last delivery id
      
      for (ClientConsumerInternal consumer: consumers.values())
      {
         consumer.recover(lastID + 1);
      }
      
      acknowledgeInternal(false);      

      remotingConnection.send(id, new SessionRollbackMessage());
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
         remotingConnection.send(id, new SessionCancelMessage(lastID, true), true);
         
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

   public synchronized void close() throws MessagingException
   {
      if (closed)
      {
         return;
      }

      try
      {
         closeChildren();
         
         //Make sure any remaining acks make it to the server
         
         acknowledgeInternal(false);      
         
         remotingConnection.send(id, new CloseMessage());
   
         executor.shutdownNow();
      }
      finally
      {
         connection.removeChild(id);
         
         closed = true;
      }
   }
  
   public boolean isClosed()
   {
      return closed;
   }
   
   // ClientSessionInternal implementation ------------------------------------------------------------
   
   public String getID()
   {
      return id;
   }
   
   public ClientConnectionInternal getConnection()
   {
      return connection;
   }

   public void delivered(long deliverID, boolean expired)
   {
      this.deliverID = deliverID;
      
      this.deliveryExpired = expired;
   }
   
   public void flushAcks() throws MessagingException
   {
      acknowledgeInternal(false);
   }
   
   public void removeConsumer(ClientConsumerInternal consumer) throws MessagingException
   {
      consumers.remove(consumer.getID());
            
      //1. flush any unacked message to the server
      
      acknowledgeInternal(false);
      
      //2. cancel all deliveries on server but not in tx
            
      remotingConnection.send(id, new SessionCancelMessage(-1, false));      
   }
   
   public void removeProducer(ClientProducer producer)
   {
      producers.remove(producer);
   }
   
   public void removeBrowser(ClientBrowser browser)
   {
      browsers.remove(browser);
   }
   
   public void send(String address, Message m) throws MessagingException
   {
      checkClosed();
      
      SessionSendMessage message = new SessionSendMessage(address, m);
      
      remotingConnection.send(id, message, !m.isDurable());
   }
      
   // XAResource implementation --------------------------------------------------------------------
   
   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      try
      { 
         SessionXACommitMessage packet = new SessionXACommitMessage(xid, onePhase);
                  
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.send(id, packet);
         
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

   public void end(Xid xid, int flags) throws XAException
   {
      try
      {
         AbstractPacket packet;
         
         if (flags == XAResource.TMSUSPEND)
         {
            packet = new SessionXASuspendMessage();                  
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
         
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.send(id, packet);
         
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

   public void forget(Xid xid) throws XAException
   {
      try
      {                              
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.send(id, new SessionXAForgetMessage(xid));
         
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
            (SessionXAGetTimeoutResponseMessage)remotingConnection.send(id, new SessionXAGetTimeoutMessage());
         
         return response.getTimeoutSeconds();
      }
      catch (MessagingException e)
      {
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public boolean isSameRM(XAResource xares) throws XAException
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
      
      return this.connection.getServerID() == other.getConnection().getServerID();
   }

   public int prepare(Xid xid) throws XAException
   {
      try
      {
         SessionXAPrepareMessage packet = new SessionXAPrepareMessage(xid);
         
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.send(id, packet);
         
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

   public Xid[] recover(int flag) throws XAException
   {
      try
      {
         SessionXAGetInDoubtXidsMessage packet = new SessionXAGetInDoubtXidsMessage();
         
         SessionXAGetInDoubtXidsResponseMessage response = (SessionXAGetInDoubtXidsResponseMessage)remotingConnection.send(id, packet);
         
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

   public void rollback(Xid xid) throws XAException
   {
      try
      {

         SessionXARollbackMessage packet = new SessionXARollbackMessage(xid);
         
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.send(id, packet);
         
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

   public boolean setTransactionTimeout(int seconds) throws XAException
   {
      try
      {                              
         SessionXASetTimeoutResponseMessage response =
            (SessionXASetTimeoutResponseMessage)remotingConnection.send(id, new SessionXASetTimeoutMessage(seconds));
         
         return response.isOK();
      }
      catch (MessagingException e)
      {
         //This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public void start(Xid xid, int flags) throws XAException
   {
      try
      {
         AbstractPacket packet;
         
         if (flags == XAResource.TMJOIN)
         {
            packet = new SessionXAJoinMessage(xid);                  
         }
         else if (flags == XAResource.TMRESUME)
         {
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
                     
         SessionXAResponseMessage response = (SessionXAResponseMessage)remotingConnection.send(id, packet);
         
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
  
   public void setForceNotSameRM(boolean force)
   {
      this.forceNotSameRM = force;
   }
   
   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void acknowledgeInternal(boolean block) throws MessagingException
   {
      if (acked)
      {
         return;
      }
      
      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(lastID, !broken);
      
      remotingConnection.send(id, message, !block);
      
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
