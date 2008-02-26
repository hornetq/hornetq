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
package org.jboss.messaging.core.server.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.server.Binding;
import org.jboss.messaging.core.server.Delivery;
import org.jboss.messaging.core.server.Filter;
import org.jboss.messaging.core.server.Message;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.MessagingException;
import org.jboss.messaging.core.server.PersistenceManager;
import org.jboss.messaging.core.server.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ResourceManager;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerProducer;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.server.Transaction;

/**
 * Session implementation
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Parts derived from
 *         JBM 1.x ServerSessionImpl by
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision: 3783 $</tt>
 *
 * $Id: ServerSessionImpl.java 3783 2008-02-25 12:15:14Z timfox $
 */
public class ServerSessionImpl implements ServerSession
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionImpl.class);

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private final boolean trace = log.isTraceEnabled();

   private final String id;
   
   private final boolean autoCommitSends;

   private final boolean autoCommitAcks;
   
   private final ServerConnection connection;
   
   private final ResourceManager resourceManager;

   private final PacketSender sender;
   
   private final PacketDispatcher dispatcher;
   
   private final PersistenceManager persistenceManager;
   
   private final PostOffice postOffice;
   
   private final SecurityStore securityStore;
         
   private final Map<String, ServerConsumer> consumers = new ConcurrentHashMap<String, ServerConsumer>();

   private final Map<String, ServerBrowserImpl> browsers = new ConcurrentHashMap<String, ServerBrowserImpl>();
   
   private final Map<String, ServerProducer> producers = new ConcurrentHashMap<String, ServerProducer>();

   private final LinkedList<Delivery> deliveries = new LinkedList<Delivery>();

   private long deliveryIDSequence = 0;

   private final ExecutorService executor = Executors.newSingleThreadExecutor();

   private Transaction tx;

   // Constructors
   // ---------------------------------------------------------------------------------

   public ServerSessionImpl(final boolean autoCommitSends,
                            final boolean autoCommitAcks, final int prefetchSize,
                            final boolean xa, final ServerConnection connection,
                            final ResourceManager resourceManager, final PacketSender sender, 
                            final PacketDispatcher dispatcher, final PersistenceManager persistenceManager,
                            final PostOffice postOffice, final SecurityStore securityStore) throws Exception
   {
   	id = UUID.randomUUID().toString();
            
      this.autoCommitSends = autoCommitSends;

      this.autoCommitAcks = autoCommitAcks;
      
      if (!xa)
      {
         tx = new TransactionImpl();
      }

      this.connection = connection;

      this.resourceManager = resourceManager;
            
      this.sender = sender;
    
      this.dispatcher = dispatcher;
      
      this.persistenceManager = persistenceManager;
      
      this.postOffice = postOffice;
      
      this.securityStore = securityStore;
            
      if (log.isTraceEnabled())
      {
         log.trace("created server session endpoint for " + sender.getRemoteAddress());
      }
   }

   // ServerSession implementation
   // ---------------------------------------------------------------------------------------
   
   public String getID()
   {
   	return id;
   }
   
   public ServerConnection getConnection()
   {
      return connection;
   }

   public void removeBrowser(final String browserID) throws Exception
   {
      if (browsers.remove(browserID) == null)
      {
         throw new IllegalStateException("Cannot find browser with id " + browserID + " to remove");
      }
      
      dispatcher.unregister(browserID);           
   }

   public void removeConsumer(final String consumerID) throws Exception
   {
      if (consumers.remove(consumerID) == null)
      {
         throw new IllegalStateException("Cannot find consumer with id " + consumerID + " to remove");
      }
      
      dispatcher.unregister(consumerID);           
   }
   
   public void removeProducer(final String producerID) throws Exception
   {
      if (producers.remove(producerID) == null)
      {
         throw new IllegalStateException("Cannot find producer with id " + producerID + " to remove");
      }
      
      dispatcher.unregister(producerID);           
   }
   
   public synchronized void handleDelivery(final MessageReference ref, final ServerConsumer consumer) throws Exception
   {
      Delivery delivery = new DeliveryImpl(ref, consumer.getID(), deliveryIDSequence++, sender);

      deliveries.add(delivery);

      delivery.deliver();
   }
   
   public void setStarted(final boolean s) throws Exception
   {
      Map<String, ServerConsumer> consumersClone = new HashMap<String, ServerConsumer>(consumers);

      for (ServerConsumer consumer: consumersClone.values())
      {
         consumer.setStarted(s);
      }
   }

   public void close() throws Exception
   {
      Map<String, ServerConsumer> consumersClone = new HashMap<String, ServerConsumer>(consumers);

      for (ServerConsumer consumer: consumersClone.values())
      {
         consumer.close();
      }

      consumers.clear();

      Map<String, ServerBrowserImpl> browsersClone = new HashMap<String, ServerBrowserImpl>(browsers);

      for (ServerBrowserImpl browser: browsersClone.values())
      {
         browser.close();
      }

      browsers.clear();
      
      Map<String, ServerProducer> producersClone = new HashMap<String, ServerProducer>(producers);

      for (ServerProducer producer: producersClone.values())
      {
         producer.close();
      }

      producers.clear();
      
      rollback();

      executor.shutdown();

      deliveries.clear();

      connection.removeSession(id);
   }
   
   public void promptDelivery(final Queue queue)
   {
      // TODO - do we really need to prompt on a different thread?
      executor.execute(new Runnable()
      {
         public void run()
         {
            queue.deliver();
         }
      });
   }
   
   public void send(final String address, final Message msg) throws Exception
   {
      //check the address exists, if it doesnt add if the user has the correct privileges
      if (!postOffice.containsAllowableAddress(address))
      {
         try
         {
            securityStore.check(address, CheckType.CREATE, connection);
            
            postOffice.addAllowableAddress(address);
         }
         catch (MessagingException e)
         {
            throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
         }
      }
      //check the user has write access to this address
      securityStore.check(address, CheckType.WRITE, connection);
      // Assign the message an internal id - this is used to key it in the store
      msg.setMessageID(persistenceManager.generateMessageID());

      // This allows the no-local consumers to filter out the messages that come
      // from the same
      // connection.

      msg.setConnectionID(connection.getID());

      postOffice.route(address, msg);

      if (!msg.getReferences().isEmpty())
      {
         if (autoCommitSends)
         {
            if (msg.getNumDurableReferences() != 0)
            {
               persistenceManager.addMessage(msg);
            }

            msg.send();
         }
         else
         {
            tx.addMessage(msg);
         }
      }
   }

   public synchronized void acknowledge(final long deliveryID, final boolean allUpTo) throws Exception
   {
      // Note that we do not consider it an error if the deliveries cannot be
      // found to be acked.
      // This can legitimately occur if a connection/session/consumer is closed
      // from inside a MessageHandlers
      // onMessage method. In this situation the close will cancel any unacked
      // deliveries, but the subsequent
      // call to delivered() will try and ack again and not find the last
      // delivery on the server.
      if (allUpTo)
      {
         // Ack all deliveries up to and including the specified id

         for (Iterator<Delivery> iter = deliveries.iterator(); iter.hasNext();)
         {
            Delivery rec = iter.next();

            if (rec.getDeliveryID() <= deliveryID)
            {
               iter.remove();

               MessageReference ref = rec.getReference();

               if (rec.getDeliveryID() > deliveryID)
               {
                  // This catches the case where the delivery has been cancelled
                  // since it's expired
                  // And we don't want to end up acking all deliveries!
                  break;
               }

               if (autoCommitAcks)
               {
                  ref.acknowledge(persistenceManager);
               }
               else
               {
                  tx.addAcknowledgement(ref);

                  //Del count is not actually updated in storage unless it's cancelled
                  ref.incrementDeliveryCount();
               }

               if (rec.getDeliveryID() == deliveryID)
               {
                  break;
               }
            }
            else
            {
               // Sanity check
               throw new IllegalStateException("Failed to ack contiguently");
            }
         }
      }
      else
      {
         // Ack a specific delivery

         for (Iterator<Delivery> iter = deliveries.iterator(); iter.hasNext();)
         {
            Delivery rec = iter.next();

            if (rec.getDeliveryID() == deliveryID)
            {
               iter.remove();

               MessageReference ref = rec.getReference();

               if (autoCommitAcks)
               {
                  ref.acknowledge(persistenceManager);
               }
               else
               {
                  tx.addAcknowledgement(ref);

                  //Del count is not actually updated in storage unless it's cancelled
                  ref.incrementDeliveryCount();
               }

               break;
            }
         }
      }
   }

   public void rollback() throws Exception
   {
      if (tx == null)
      {
         // Might be null if XA

         tx = new TransactionImpl();
      }

      // Synchronize to prevent any new deliveries arriving during this recovery
      synchronized (this)
      {
         // Add any unacked deliveries into the tx
         // Doing this ensures all references are rolled back in the correct
         // orderin a single contiguous block

         for (Delivery del : deliveries)
         {
            tx.addAcknowledgement(del.getReference());
         }

         deliveries.clear();

         deliveryIDSequence -= tx.getAcknowledgementsCount();
      }

      tx.rollback(persistenceManager);
   }

   public void cancel(final long deliveryID, final boolean expired) throws Exception
   {
      if (deliveryID == -1)
      {
         // Cancel all

         Transaction cancelTx;

         synchronized (this)
         {
            cancelTx = new TransactionImpl();

            for (Delivery del : deliveries)
            {
               cancelTx.addAcknowledgement(del.getReference());
            }

            deliveries.clear();
         }

         cancelTx.rollback(persistenceManager);
      }
      else if (expired)
      {
         if (deliveryID == -1)
         {
            throw new IllegalArgumentException("Invalid delivery id");
         }

         // Expire a single reference

         for (Iterator<Delivery> iter = deliveries.iterator(); iter.hasNext();)
         {
            Delivery delivery = iter.next();

            if (delivery.getDeliveryID() == deliveryID)
            {
               delivery.getReference().expire(persistenceManager);

               iter.remove();

               break;
            }
         }
      }
      else
      {
         throw new IllegalArgumentException("Invalid delivery id " + deliveryID);
      }
   }

   public void commit() throws Exception
   {
      tx.commit(true, persistenceManager);
   }

   public SessionXAResponseMessage XACommit(final boolean onePhase, final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot commit, session is currently doing work in a transaction "
               + tx.getXid();

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      Transaction theTx = resourceManager.getTransaction(xid);

      if (theTx == null)
      {
         final String msg = "Cannot find xid in resource manager: " + xid;

         return new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
      }

      if (theTx.isSuspended()) { return new SessionXAResponseMessage(true,
            XAException.XAER_PROTO,
            "Cannot commit transaction, it is suspended " + xid); }

      theTx.commit(onePhase, persistenceManager);

      boolean removed = resourceManager.removeTransaction(xid);

      if (!removed)
      {
         final String msg = "Failed to remove transaction: " + xid;

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   public SessionXAResponseMessage XAEnd(final Xid xid, final boolean failed) throws Exception
   {
      if (tx != null && tx.getXid().equals(xid))
      {
         if (tx.isSuspended())
         {
            final String msg = "Cannot end, transaction is suspended";

            return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
         }

         tx = null;
      }
      else
      {
         // It's also legal for the TM to call end for a Xid in the suspended
         // state
         // See JTA 1.1 spec 3.4.4 - state diagram
         // Although in practice TMs rarely do this.
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null)
         {
            final String msg = "Cannot find suspended transaction to end "
                  + xid;

            return new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
         }

         if (!theTx.isSuspended())
         {
            final String msg = "Transaction is not suspended " + xid;

            return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
         }

         theTx.resume();
      }

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   public SessionXAResponseMessage XAForget(final Xid xid)
   {
      // Do nothing since we don't support heuristic commits / rollback from the
      // resource manager

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   public SessionXAResponseMessage XAJoin(final Xid xid) throws Exception
   {
      Transaction theTx = resourceManager.getTransaction(xid);

      if (theTx == null)
      {
         final String msg = "Cannot find xid in resource manager: " + xid;

         return new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
      }

      if (theTx.isSuspended()) { return new SessionXAResponseMessage(true,
            XAException.XAER_PROTO, "Cannot join tx, it is suspended " + xid); }

      tx = theTx;

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   public SessionXAResponseMessage XAPrepare(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot commit, session is currently doing work in a transaction "
               + tx.getXid();

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      Transaction theTx = resourceManager.getTransaction(xid);

      if (theTx == null)
      {
         final String msg = "Cannot find xid in resource manager: " + xid;

         return new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
      }

      if (theTx.isSuspended()) { return new SessionXAResponseMessage(true,
            XAException.XAER_PROTO,
            "Cannot prepare transaction, it is suspended " + xid); }

      if (theTx.isEmpty())
      {
         // Nothing to do - remove it

         boolean removed = resourceManager.removeTransaction(xid);

         if (!removed)
         {
            final String msg = "Failed to remove transaction: " + xid;

            return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
         }

         return new SessionXAResponseMessage(false, XAResource.XA_RDONLY, null);
      }
      else
      {
         theTx.prepare(persistenceManager);

         return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
      }
   }

   public SessionXAResponseMessage XAResume(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot resume, session is currently doing work in a transaction "
               + tx.getXid();

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      Transaction theTx = resourceManager.getTransaction(xid);

      if (theTx == null)
      {
         final String msg = "Cannot find xid in resource manager: " + xid;

         return new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
      }

      if (!theTx.isSuspended()) { return new SessionXAResponseMessage(true,
            XAException.XAER_PROTO,
            "Cannot resume transaction, it is not suspended " + xid); }

      tx = theTx;

      tx.resume();

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   public SessionXAResponseMessage XARollback(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot roll back, session is currently doing work in a transaction "
               + tx.getXid();

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      Transaction theTx = resourceManager.getTransaction(xid);

      if (theTx == null)
      {
         final String msg = "Cannot find xid in resource manager: " + xid;

         return new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
      }

      if (theTx.isSuspended()) { return new SessionXAResponseMessage(true,
            XAException.XAER_PROTO,
            "Cannot rollback transaction, it is suspended " + xid); }

      theTx.rollback(persistenceManager);

      boolean removed = resourceManager.removeTransaction(xid);

      if (!removed)
      {
         final String msg = "Failed to remove transaction: " + xid;

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   public SessionXAResponseMessage XAStart(final Xid xid)
   {
      if (tx != null)
      {
         final String msg = "Cannot start, session is already doing work in a transaction "
               + tx.getXid();

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      tx = new TransactionImpl(xid);

      boolean added = resourceManager.putTransaction(xid, tx);

      if (!added)
      {
         final String msg = "Cannot start, there is already a xid "
               + tx.getXid();

         return new SessionXAResponseMessage(true, XAException.XAER_DUPID, msg);
      }

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   public SessionXAResponseMessage XASuspend() throws Exception
   {
      if (tx == null)
      {
         final String msg = "Cannot suspend, session is not doing work in a transaction "
               + tx.getXid();

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      if (tx.isSuspended())
      {
         final String msg = "Cannot suspend, transaction is already suspended "
               + tx.getXid();

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      tx.suspend();

      tx = null;

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   public List<Xid> getInDoubtXids() throws Exception
   {
      return null;
   }

   public int getXATimeout()
   {
      return resourceManager.getTimeoutSeconds();
   }

   public boolean setXATimeout(int timeoutSeconds)
   {
      return resourceManager.setTimeoutSeconds(timeoutSeconds);
   }

   public void addAddress(final String address) throws Exception
   {
      if (postOffice.containsAllowableAddress(address))
      {
         throw new MessagingException(MessagingException.ADDRESS_EXISTS, "Address already exists: " + address);
      }
      
      securityStore.check(address, CheckType.CREATE, connection);
      
      postOffice.addAllowableAddress(address);
   }

   public void removeAddress(final String address) throws Exception
   {
      if (!postOffice.removeAllowableAddress(address))
      {
         throw new MessagingException(MessagingException.ADDRESS_DOES_NOT_EXIST, "Address does not exist: " + address);
      }
   }

   public void createQueue(final String address, final String queueName,
         final String filterString, boolean durable, final boolean temporary) throws Exception
   {
      //make sure the user has privileges to create this address
      if (!postOffice.containsAllowableAddress(address))
      {
         try
         {
         	securityStore.check(address, CheckType.CREATE, connection);
         	
            postOffice.addAllowableAddress(address);
         }
         catch (MessagingException e)
         {
            throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
         }
      }
      Binding binding = postOffice.getBinding(queueName);

      if (binding != null)
      {
         throw new MessagingException(MessagingException.QUEUE_EXISTS);
      }

      if (temporary)
      {
         durable = false;
      }

      Filter filter = null;

      if (filterString != null)
      {
         filter = new FilterImpl(filterString);
      }

      binding = postOffice.addBinding(address, queueName, filter, durable,
            temporary);

      if (temporary)
      {
         Queue queue = binding.getQueue();

         connection.addTemporaryQueue(queue);
      }
   }

   public void deleteQueue(final String queueName) throws Exception
   {
      Binding binding = postOffice.removeBinding(queueName);

      if (binding == null)
      {
         throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
      }

      Queue queue = binding.getQueue();

      if (queue.isDurable())
      {
      	persistenceManager.deleteAllReferences(binding.getQueue());
      }

      if (queue.isTemporary())
      {
         connection.removeTemporaryQueue(queue);
      }
   }

   public SessionCreateConsumerResponseMessage
      createConsumer(final String queueName, final String filterString,
                     final boolean noLocal, final boolean autoDeleteQueue, final int prefetchSize) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
      }
      
      securityStore.check(binding.getAddress(), CheckType.READ, connection);
      
      Filter filter = null;

      if (filterString != null)
      {
         filter = new FilterImpl(filterString);
      }

      ServerConsumer consumer =
      	new ServerConsumerImpl(binding.getQueue(), noLocal, filter, autoDeleteQueue, prefetchSize > 0, connection.getID(),
                                    this, persistenceManager, postOffice, connection.isStarted());

      dispatcher.register(new ServerConsumerPacketHandler(consumer));

      SessionCreateConsumerResponseMessage response = new SessionCreateConsumerResponseMessage(consumer.getID(),
            prefetchSize);

      consumers.put(consumer.getID(), consumer);      

      log.trace(this + " created and registered " + consumer);

      return response;
   }

   public SessionQueueQueryResponseMessage executeQueueQuery(final SessionQueueQueryMessage request) throws Exception
   {
      if (request.getQueueName() == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }

      Binding binding = postOffice.getBinding(request.getQueueName());

      SessionQueueQueryResponseMessage response;

      if (binding != null)
      {
         Queue queue = binding.getQueue();

         Filter filter = queue.getFilter();

         String filterString = filter == null ? null : filter.getFilterString();

         response = new SessionQueueQueryResponseMessage(queue.isDurable(), queue.isTemporary(), queue.getMaxSize(),
                                           queue.getConsumerCount(), queue.getMessageCount(),
                                           filterString, binding.getAddress());
      }
      else
      {
         response = new SessionQueueQueryResponseMessage();
      }

      return response;
   }

   public SessionBindingQueryResponseMessage executeBindingQuery(final SessionBindingQueryMessage request) throws Exception
   {
      if (request.getAddress() == null)
      {
         throw new IllegalArgumentException("Address is null");
      }

      boolean exists = postOffice.containsAllowableAddress(request.getAddress());

      List<String> queueNames = new ArrayList<String>();

      if (exists)
      {
         List<Binding> bindings = postOffice.getBindingsForAddress(request.getAddress());

         for (Binding binding: bindings)
         {
            queueNames.add(binding.getQueue().getName());
         }
      }

      return new SessionBindingQueryResponseMessage(exists, queueNames);
   }

   public SessionCreateBrowserResponseMessage createBrowser(final String queueName, final String selector)
         throws Exception
   {
      if (!postOffice.containsAllowableAddress(queueName))
      {
         try
         {
         	securityStore.check(queueName, CheckType.CREATE, connection);
            
            postOffice.addAllowableAddress(queueName);
         }
         catch (MessagingException e)
         {
            throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
         }
      }
      
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
      }
      
      securityStore.check(binding.getAddress(), CheckType.READ, connection);
      
      ServerBrowserImpl browser = new ServerBrowserImpl(this, binding.getQueue(), selector);

      browsers.put(browser.getID(), browser);
      
      dispatcher.register(browser.newHandler());

      return new SessionCreateBrowserResponseMessage(browser.getID());
   }
   
   public SessionCreateProducerResponseMessage createProducer(final String address) throws Exception
   {
   	ServerProducerImpl producer = new ServerProducerImpl(this, address);
   	
   	producers.put(producer.getID(), producer);
   	
   	dispatcher.register(new ServerProducerPacketHandler(producer));
   	
   	return new SessionCreateProducerResponseMessage(producer.getID());
   }
   
   // Public ---------------------------------------------------------------------------------------------
   
   public String toString()
   {
      return "SessionEndpoint[" + id + "]";
   }  
}
