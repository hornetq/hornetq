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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.message.ServerMessage;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketSender;
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
import org.jboss.messaging.core.server.Delivery;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerProducer;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.SimpleString;

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

   private final long id;

   private final boolean autoCommitSends;

   private final boolean autoCommitAcks;

   private final ServerConnection connection;

   private final ResourceManager resourceManager;

   private final PacketSender sender;

   private final PacketDispatcher dispatcher;

   private final StorageManager persistenceManager;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final PostOffice postOffice;

   private final SecurityStore securityStore;

   private final Set<ServerConsumer> consumers = new ConcurrentHashSet<ServerConsumer>();

   private final Set<ServerBrowserImpl> browsers = new ConcurrentHashSet<ServerBrowserImpl>();

   private final Set<ServerProducer> producers = new ConcurrentHashSet<ServerProducer>();

   private final LinkedList<Delivery> deliveries = new LinkedList<Delivery>();

   private long deliveryIDSequence = 0;

   private final ExecutorService executor = Executors.newSingleThreadExecutor();

   private Transaction tx;

   private ArrayList<Queue> stopped = new ArrayList<Queue>();

   // Constructors
   // ---------------------------------------------------------------------------------

   public ServerSessionImpl(final long id, final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean xa, final ServerConnection connection,
                            final ResourceManager resourceManager, final PacketSender sender,
                            final PacketDispatcher dispatcher, final StorageManager persistenceManager,
                            final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                            final PostOffice postOffice, final SecurityStore securityStore) throws Exception
   {
   	this.id = id;

      this.autoCommitSends = autoCommitSends;

      this.autoCommitAcks = autoCommitAcks;

      if (!xa)
      {
         tx = new TransactionImpl(persistenceManager, postOffice);
      }

      this.connection = connection;

      this.resourceManager = resourceManager;

      this.sender = sender;

      this.dispatcher = dispatcher;

      this.persistenceManager = persistenceManager;

      this.queueSettingsRepository = queueSettingsRepository;

      this.postOffice = postOffice;

      this.securityStore = securityStore;

      if (log.isTraceEnabled())
      {
         log.trace("created server session endpoint for " + sender.getRemoteAddress());
      }
   }

   // ServerSession implementation
   // ---------------------------------------------------------------------------------------

   public long getID()
   {
   	return id;
   }

   public ServerConnection getConnection()
   {
      return connection;
   }

   public void removeBrowser(final ServerBrowserImpl browser) throws Exception
   {
      if (!browsers.remove(browser))
      {
         throw new IllegalStateException("Cannot find browser with id " + browser.getID() + " to remove");
      }

      dispatcher.unregister(browser.getID());
   }

   public void removeConsumer(final ServerConsumer consumer) throws Exception
   {
      if (!consumers.remove(consumer))
      {
         throw new IllegalStateException("Cannot find consumer with id " + consumer.getID() + " to remove");
      }

      dispatcher.unregister(consumer.getID());
   }

   public void removeProducer(final ServerProducer producer) throws Exception
   {
      if (!producers.remove(producer))
      {
         throw new IllegalStateException("Cannot find producer with id " + producer.getID() + " to remove");
      }

      dispatcher.unregister(producer.getID());
   }

   public synchronized HandleStatus handleDelivery(final MessageReference ref, final ServerConsumer consumer) throws Exception
   {
      //if the queue we are delivering to has been stopped then dont deliver!
      if (stopped.contains(ref.getQueue()))
      {
         return HandleStatus.BUSY;
      }
      Delivery delivery = new DeliveryImpl(ref, id, consumer.getClientTargetID(), deliveryIDSequence++, sender);

      deliveries.add(delivery);

      delivery.deliver();
      return HandleStatus.HANDLED;
   }

   public void setStarted(final boolean s) throws Exception
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers);

      for (ServerConsumer consumer: consumersClone)
      {
         consumer.setStarted(s);
      }
   }

   public void close() throws Exception
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers);

      for (ServerConsumer consumer: consumersClone)
      {
         consumer.close();
      }

      consumers.clear();

      Set<ServerBrowserImpl> browsersClone = new HashSet<ServerBrowserImpl>(browsers);

      for (ServerBrowserImpl browser: browsersClone)
      {
         browser.close();
      }

      browsers.clear();

      Set<ServerProducer> producersClone = new HashSet<ServerProducer>(producers);

      for (ServerProducer producer: producersClone)
      {
         producer.close();
      }

      producers.clear();

      rollback();

      executor.shutdown();

      deliveries.clear();

      connection.removeSession(this);
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

   public void send(final ServerMessage msg) throws Exception
   {
      //check the user has write access to this address
      securityStore.check(msg.getDestination().toString(), CheckType.WRITE, connection);

      msg.setMessageID(persistenceManager.generateMessageID());

      // This allows the no-local consumers to filter out the messages that come
      // from the same connection.

      msg.setConnectionID(connection.getID());

      if (autoCommitSends)
      {
      	List<MessageReference> refs = postOffice.route(msg);

   		if (msg.getDurableRefCount() != 0)
   		{
   			persistenceManager.storeMessage(msg);
   		}

   		for (MessageReference ref: refs)
   		{
   			ref.getQueue().addLast(ref);
   		}
      }
      else
      {
      	tx.addMessage(msg);
      }
   }

   public synchronized void acknowledge(final long deliveryID, final boolean allUpTo) throws Exception
   {
   	/*
       Note that we do not consider it an error if the deliveries cannot be found to be acked.
   	 This can legitimately occur if a connection/session/consumer is closed
       from inside a MessageHandlers onMessage method. In this situation the close will cancel any unacked
       deliveries, but the subsequent call to delivered() will try and ack again and not find the last
       delivery on the server.
       */
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
               	doAck(ref);
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
               	doAck(ref);
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

         tx = new TransactionImpl(persistenceManager, postOffice);
      }

      // Synchronize to prevent any new deliveries arriving during this recovery. also we stop any queues that are having
      // messages rolled back, if their are any messages in mid delivery then these will not be delivered. 
      synchronized (this)
      {
         // Add any unacked deliveries into the tx. Doing this ensures all references are rolled back in the correct
         // order in a single contiguous block

         for (Delivery del : deliveries)
         {
            tx.addAcknowledgement(del.getReference());
         }
         //stop the queue delivering for all the queues where messages are being rolled back
         List<MessageReference> acks = tx.getAcknowledgements();
         for (MessageReference ack : acks)
         {
            stopped.add(ack.getQueue());
         }
         for (Queue queue : stopped)
         {
            queue.stopDelivery();
         }
         deliveries.clear();
         deliveryIDSequence -= tx.getAcknowledgementsCount();
      }
      tx.rollback(queueSettingsRepository);
      //once we have done the rollbackwe can restart any queues which will flush any awaiting deliveries
      ArrayList<Queue> toRestart = new ArrayList<Queue>(stopped);
      stopped.clear();
      for (Queue queue : toRestart)
      {
         queue.startDelivery();
      }

      tx = new TransactionImpl(persistenceManager, postOffice);
   }

   public void cancel(final long deliveryID, final boolean expired) throws Exception
   {
      if (deliveryID == -1)
      {
         // Cancel all

         Transaction cancelTx;

         synchronized (this)
         {
            cancelTx = new TransactionImpl(persistenceManager, postOffice);

            for (Delivery del : deliveries)
            {
               cancelTx.addAcknowledgement(del.getReference());
            }

            deliveries.clear();
         }

         cancelTx.rollback(queueSettingsRepository);
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
               delivery.getReference().expire(persistenceManager, postOffice, queueSettingsRepository);

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
      tx.commit();

      tx = new TransactionImpl(persistenceManager, postOffice);
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

      if (theTx.getState() == Transaction.State.SUSPENDED) { return new SessionXAResponseMessage(true,
            XAException.XAER_PROTO,
            "Cannot commit transaction, it is suspended " + xid); }

      theTx.commit();

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
         if (tx.getState() == Transaction.State.SUSPENDED)
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

         if (theTx.getState() != Transaction.State.SUSPENDED)
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

      if (theTx.getState() == Transaction.State.SUSPENDED) { return new SessionXAResponseMessage(true,
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

      if (theTx.getState() == Transaction.State.SUSPENDED) { return new SessionXAResponseMessage(true,
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
         theTx.prepare();

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

      if (theTx.getState() != Transaction.State.SUSPENDED) { return new SessionXAResponseMessage(true,
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

      if (theTx.getState() == Transaction.State.SUSPENDED) { return new SessionXAResponseMessage(true,
            XAException.XAER_PROTO,
            "Cannot rollback transaction, it is suspended " + xid); }

      theTx.rollback(queueSettingsRepository);

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

      tx = new TransactionImpl(xid, persistenceManager, postOffice);

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

      if (tx.getState() == Transaction.State.SUSPENDED)
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

   public void addDestination(final SimpleString address, final boolean temporary) throws Exception
   {
      securityStore.check(address.toString(), CheckType.CREATE, connection);

      if (!postOffice.addDestination(address, temporary))
      {
      	throw new MessagingException(MessagingException.ADDRESS_EXISTS, "Address already exists: " + address);
      }
      else
      {
         if (temporary)
         {
            connection.addTemporaryDestination(address);
         }
      }
   }

   public void removeDestination(final SimpleString address, final boolean temporary) throws Exception
   {
   	securityStore.check(address.toString(), CheckType.CREATE, connection);

      if (!postOffice.removeDestination(address, temporary))
      {
         throw new MessagingException(MessagingException.ADDRESS_DOES_NOT_EXIST, "Address does not exist: " + address);
      }
      else
      {
         if (temporary)
         {
         	connection.removeTemporaryDestination(address);
         }
      }
   }

   public void createQueue(final SimpleString address, final SimpleString queueName,
         final SimpleString filterString, boolean durable, final boolean temporary) throws Exception
   {
      //make sure the user has privileges to create this address
      if (!postOffice.containsDestination(address))
      {
         try
         {
         	securityStore.check(address.toString(), CheckType.CREATE, connection);
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

   public void deleteQueue(final SimpleString queueName) throws Exception
   {
      Binding binding = postOffice.removeBinding(queueName);

      if (binding == null)
      {
         throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
      }

      Queue queue = binding.getQueue();

      if (queue.getConsumerCount() != 0)
      {
      	throw new MessagingException(MessagingException.ILLEGAL_STATE, "Cannot delete queue - it has consumers");
      }

      if (queue.isDurable())
      {
      	binding.getQueue().deleteAllReferences(persistenceManager);
      }

      if (queue.isTemporary())
      {
         connection.removeTemporaryQueue(queue);
      }
   }

   public SessionCreateConsumerResponseMessage createConsumer(final long clientTargetID, final SimpleString queueName, final SimpleString filterString,
                                                              final boolean noLocal, final boolean autoDeleteQueue,
                                                              int windowSize, int maxRate) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
      }

      securityStore.check(binding.getAddress().toString(), CheckType.READ, connection);

      Filter filter = null;

      if (filterString != null)
      {
         filter = new FilterImpl(filterString);
      }

      //Flow control values if specified on queue override those passed in from client

      Integer queueWindowSize = queueSettingsRepository.getMatch(queueName.toString()).getConsumerWindowSize();

      windowSize = queueWindowSize != null ? queueWindowSize : windowSize;

      Integer queueMaxRate = queueSettingsRepository.getMatch(queueName.toString()).getConsumerMaxRate();

      maxRate = queueMaxRate != null ? queueMaxRate : maxRate;

      long id = dispatcher.generateID();
      
      ServerConsumer consumer =
      	new ServerConsumerImpl(id, clientTargetID, binding.getQueue(), noLocal, filter, autoDeleteQueue, windowSize != -1, maxRate, connection.getID(),
                                this, persistenceManager, queueSettingsRepository, postOffice, connection.isStarted());

      dispatcher.register(new ServerConsumerPacketHandler(consumer));

      SessionCreateConsumerResponseMessage response =
      	new SessionCreateConsumerResponseMessage(consumer.getID(), windowSize);

      consumers.add(consumer);

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

         SimpleString filterString = filter == null ? null : filter.getFilterString();

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

      boolean exists = postOffice.containsDestination(request.getAddress());

      List<SimpleString> queueNames = new ArrayList<SimpleString>();

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

   public SessionCreateBrowserResponseMessage createBrowser(final SimpleString queueName, final SimpleString filterString)
         throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
      }

      securityStore.check(binding.getAddress().toString(), CheckType.READ, connection);

      long id = dispatcher.generateID();
      
      ServerBrowserImpl browser = new ServerBrowserImpl(id, this, binding.getQueue(), filterString == null ? null : filterString.toString());

      browsers.add(browser);

      dispatcher.register(browser.newHandler());

      return new SessionCreateBrowserResponseMessage(browser.getID());
   }
   
   /**
    * Create a producer for the specified address
    * @param address The address to produce too
    * @param windowSize - the producer window size to use for flow control.
    * Specify -1 to disable flow control completely
    * The actual window size used may be less than the specified window size if the queue's maxSize attribute
    * is set and there are not sufficient empty spaces in the queue, or it is overridden by any producer-window_size
    * specified on the queue
    */
   public SessionCreateProducerResponseMessage createProducer(final long clientTargetID, final SimpleString address, final int windowSize,
   		                                                     final int maxRate) throws Exception
   {
   	FlowController flowController = null;

   	final int maxRateToUse = maxRate;

   	// TODO Flow control disabled for now
   	
//   	if (address != null)
//   	{
//   		flowController = windowSize == -1 ? null : postOffice.getFlowController(address);
//   	}
   	
   	long id = dispatcher.generateID();

   	ServerProducerImpl producer = new ServerProducerImpl(id, clientTargetID, this, address, sender, flowController);

   	producers.add(producer);

   	dispatcher.register(new ServerProducerPacketHandler(producer));

   	final int windowToUse = flowController == null ? -1 : flowController.getInitialTokens(windowSize, producer);

   	return new SessionCreateProducerResponseMessage(producer.getID(), windowToUse, maxRateToUse);
   }

   // Public ---------------------------------------------------------------------------------------------

   public String toString()
   {
      return "SessionEndpoint[" + id + "]";
   }

   // Private --------------------------------------------------------------------------------------------

   private void doAck(final MessageReference ref) throws Exception
   {
   	ServerMessage message = ref.getMessage();

   	Queue queue = ref.getQueue();

		if (message.isDurable() && queue.isDurable())
		{
			synchronized (message)
			{
				message.decrementDurableRefCount();

				if (message.getDurableRefCount() == 0)
				{
					persistenceManager.storeDelete(message.getMessageID());
				}
				else
				{
					persistenceManager.storeAcknowledge(queue.getPersistenceID(), message.getMessageID());
				}
			}
		}

		queue.referenceAcknowledged();
   }


}
