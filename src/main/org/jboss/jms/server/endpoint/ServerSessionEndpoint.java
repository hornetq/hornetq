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
package org.jboss.jms.server.endpoint;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BINDINGQUERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CANCEL;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATEQUEUE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_DELETE_QUEUE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_QUEUEQUERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_ROLLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_SEND;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_COMMIT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_END;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_FORGET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_GET_TIMEOUT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_INDOUBT_XIDS;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_JOIN;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_PREPARE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_RESUME;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_ROLLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_SET_TIMEOUT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_START;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_SUSPEND;

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

import org.jboss.jms.server.container.SecurityAspect;
import org.jboss.jms.server.security.CheckType;
import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.ResourceManager;
import org.jboss.messaging.core.Transaction;
import org.jboss.messaging.core.impl.DeliveryImpl;
import org.jboss.messaging.core.impl.TransactionImpl;
import org.jboss.messaging.core.impl.filter.FilterImpl;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionAddAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRemoveAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAStartMessage;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

/**
 * Session implementation
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Parts derived from
 *         JBM 1.x ServerSessionEndpoint by
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class ServerSessionEndpoint
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionEndpoint.class);

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private SecurityAspect security = new SecurityAspect();

   private boolean trace = log.isTraceEnabled();

   private String id;

   private ServerConnectionEndpoint connectionEndpoint;

   private MessagingServer sp;

   private Map<String, ServerConsumerEndpoint> consumers = new ConcurrentHashMap<String, ServerConsumerEndpoint>();

   private Map<String, ServerBrowserEndpoint> browsers = new ConcurrentHashMap<String, ServerBrowserEndpoint>();

   private PostOffice postOffice;

   private volatile LinkedList<Delivery> deliveries = new LinkedList<Delivery>();

   private long deliveryIDSequence = 0;

   ExecutorService executor = Executors.newSingleThreadExecutor();

   private Transaction tx;

   private boolean autoCommitSends;

   private boolean autoCommitAcks;

   private ResourceManager resourceManager;

   // Constructors
   // ---------------------------------------------------------------------------------

   ServerSessionEndpoint(String sessionID,
         ServerConnectionEndpoint connectionEndpoint, boolean autoCommitSends,
         boolean autoCommitAcks, boolean xa, ResourceManager resourceManager)
         throws Exception
   {
      this.id = sessionID;

      this.connectionEndpoint = connectionEndpoint;

      sp = connectionEndpoint.getMessagingServer();

      postOffice = sp.getPostOffice();

      if (!xa)
      {
         tx = new TransactionImpl();
      }

      this.autoCommitSends = autoCommitSends;

      this.autoCommitAcks = autoCommitAcks;

      this.resourceManager = resourceManager;
   }

   // Public
   // ---------------------------------------------------------------------------------------

   public ServerConnectionEndpoint getConnectionEndpoint()
   {
      return connectionEndpoint;
   }

   public String toString()
   {
      return "SessionEndpoint[" + id + "]";
   }

   // Package protected
   // ----------------------------------------------------------------------------

   void removeBrowser(String browserId) throws Exception
   {
      if (browsers.remove(browserId) == null)
      {
         throw new IllegalStateException("Cannot find browser with id " + browserId + " to remove");
      }      
   }

   void removeConsumer(String consumerId) throws Exception
   {
      if (consumers.remove(consumerId) == null)
      {
         throw new IllegalStateException("Cannot find consumer with id " + consumerId + " to remove");
      }      
   }
   
   synchronized void handleDelivery(MessageReference ref, ServerConsumerEndpoint consumer, PacketSender sender) throws Exception
   {
      // FIXME - we shouldn't have to pass in the packet Sender - this should be
      // creatable
      // without the consumer having to call change rate first
      Delivery delivery = new DeliveryImpl(ref, consumer.getID(),
            deliveryIDSequence++, sender);

      deliveries.add(delivery);

      delivery.deliver();
   }

   void setStarted(boolean s) throws Exception
   {
      Map<String, ServerConsumerEndpoint> consumersClone = new HashMap<String, ServerConsumerEndpoint>(consumers);
      
      for (ServerConsumerEndpoint consumer: consumersClone.values())
      {
         consumer.setStarted(s);
      }
   }

   void promptDelivery(final Queue queue)
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

   public void close() throws Exception
   {
      Map<String, ServerConsumerEndpoint> consumersClone = new HashMap<String, ServerConsumerEndpoint>(consumers);
      
      for (ServerConsumerEndpoint consumer: consumersClone.values())
      {
         consumer.close();
      }

      consumers.clear();

      Map<String, ServerBrowserEndpoint> browsersClone = new HashMap<String, ServerBrowserEndpoint>(browsers);
      
      for (ServerBrowserEndpoint browser: browsersClone.values())
      {
         browser.close();
      }

      consumers.clear();

      browsers.clear();

      rollback();

      executor.shutdown();

      deliveries.clear();

      connectionEndpoint.removeSession(id);

      connectionEndpoint.getMessagingServer().getRemotingService()
            .getDispatcher().unregister(id);
   }

   private boolean send(String address, Message msg) throws Exception
   {
      // Assign the message an internal id - this is used to key it in the store

      msg.setMessageID(sp.getPersistenceManager().generateMessageID());

      // This allows the no-local consumers to filter out the messages that come
      // from the same
      // connection.

      msg.setConnectionID(connectionEndpoint.getConnectionID());

      postOffice.route(address, msg);

      if (msg.getReferences().isEmpty())
      {
         // Didn't route anywhere

         return false;
      }
      else
      {
         if (autoCommitSends)
         {
            if (msg.getNumDurableReferences() != 0)
            {
               sp.getPersistenceManager().addMessage(msg);
            }

            msg.send();
         }
         else
         {
            tx.addMessage(msg);
         }

         return true;
      }
   }

   private synchronized void acknowledge(long deliveryID, boolean allUpTo)
         throws Exception
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
                  ref.acknowledge(sp.getPersistenceManager());
               }
               else
               {
                  tx.addAcknowledgement(ref);
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
                  ref.acknowledge(sp.getPersistenceManager());
               }
               else
               {
                  tx.addAcknowledgement(ref);
               }

               break;
            }
         }
      }
   }

   private void rollback() throws Exception
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
         // order
         // in a single contiguous block

         for (Delivery del : deliveries)
         {
            tx.addAcknowledgement(del.getReference());
         }

         deliveries.clear();

         deliveryIDSequence -= tx.getAcknowledgementsCount();
      }

      tx.rollback(sp.getPersistenceManager());
   }

   private void cancel(long deliveryID, boolean expired) throws Exception
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

         cancelTx.rollback(sp.getPersistenceManager());
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
               delivery.getReference().expire(sp.getPersistenceManager());

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

   private void commit() throws Exception
   {
      tx.commit(true, sp.getPersistenceManager());
   }

   private SessionXAResponseMessage XACommit(boolean onePhase, Xid xid)
         throws Exception
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

      theTx.commit(onePhase, sp.getPersistenceManager());

      boolean removed = resourceManager.removeTransaction(xid);

      if (!removed)
      {
         final String msg = "Failed to remove transaction: " + xid;

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   private SessionXAResponseMessage XAEnd(Xid xid, boolean failed) throws Exception
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

   private SessionXAResponseMessage XAForget(Xid xid)
   {
      // Do nothing since we don't support heuristic commits / rollback from the
      // resource manager

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   private SessionXAResponseMessage XAJoin(Xid xid) throws Exception
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

   private SessionXAResponseMessage XAPrepare(Xid xid) throws Exception
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
         theTx.prepare(sp.getPersistenceManager());

         return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
      }
   }

   private SessionXAResponseMessage XAResume(Xid xid) throws Exception
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

   private SessionXAResponseMessage XARollback(Xid xid) throws Exception
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

      theTx.rollback(sp.getPersistenceManager());

      boolean removed = resourceManager.removeTransaction(xid);

      if (!removed)
      {
         final String msg = "Failed to remove transaction: " + xid;

         return new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
      }

      return new SessionXAResponseMessage(false, XAResource.XA_OK, null);
   }

   private SessionXAResponseMessage XAStart(Xid xid)
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

   private SessionXAResponseMessage XASuspend() throws Exception
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

   private List<Xid> getInDoubtXids() throws Exception
   {
      return null;
   }

   private int getXATimeout()
   {
      return resourceManager.getTimeoutSeconds();
   }

   private boolean setXATimeout(int timeoutSeconds)
   {
      return resourceManager.setTimeoutSeconds(timeoutSeconds);
   }

   // Protected
   // ------------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void addAddress(String address) throws Exception
   {
      if (postOffice.containsAllowableAddress(address)) { throw new MessagingException(
            MessagingException.ADDRESS_EXISTS, "Address already exists: "
                  + address); }
      postOffice.addAllowableAddress(address);
   }

   private void removeAddress(String address) throws Exception
   {
      if (!postOffice.removeAllowableAddress(address)) { throw new MessagingException(
            MessagingException.ADDRESS_DOES_NOT_EXIST,
            "Address does not exist: " + address); }
   }

   private void createQueue(String address, String queueName,
         String filterString, boolean durable, boolean temporary)
         throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding != null) { throw new MessagingException(
            MessagingException.QUEUE_EXISTS); }

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

         connectionEndpoint.addTemporaryQueue(queue);
      }
   }

   private void deleteQueue(String queueName) throws Exception
   {
      Binding binding = postOffice.removeBinding(queueName);

      if (binding == null) { throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST); }

      Queue queue = binding.getQueue();

      if (queue.isDurable())
      {
         sp.getPersistenceManager().deleteAllReferences(binding.getQueue());
      }

      if (queue.isTemporary())
      {
         connectionEndpoint.removeTemporaryQueue(queue);
      }
   }

   private SessionCreateConsumerResponseMessage createConsumer(String queueName,  String filterString,
                                                 boolean noLocal, boolean autoDeleteQueue) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null) { throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST); }

      int prefetchSize = connectionEndpoint.getPrefetchSize();

      String consumerID = UUID.randomUUID().toString();

      Filter filter = null;

      if (filterString != null)
      {
         filter = new FilterImpl(filterString);
      }

      ServerConsumerEndpoint ep = new ServerConsumerEndpoint(sp, consumerID,
            binding.getQueue(), this, filter, noLocal, autoDeleteQueue, prefetchSize > 0);

      connectionEndpoint.getMessagingServer().getRemotingService()
            .getDispatcher().register(ep.newHandler());

      SessionCreateConsumerResponseMessage response = new SessionCreateConsumerResponseMessage(consumerID,
            prefetchSize);

      synchronized (consumers)
      {
         consumers.put(consumerID, ep);
      }

      log.trace(this + " created and registered " + ep);

      return response;
   }

   public SessionQueueQueryResponseMessage executeQueueQuery(SessionQueueQueryMessage request) throws Exception
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
   
   public SessionBindingQueryResponseMessage executeBindingQuery(SessionBindingQueryMessage request) throws Exception
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

   private SessionCreateBrowserResponseMessage createBrowser(String queueName, String selector)
         throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null) { throw new MessagingException(
            MessagingException.QUEUE_DOES_NOT_EXIST); }

      String browserID = UUID.randomUUID().toString();

      ServerBrowserEndpoint ep = new ServerBrowserEndpoint(this, browserID,
            binding.getQueue(), selector);

      // still need to synchronized since close() can come in on a different
      // thread
      synchronized (browsers)
      {
         browsers.put(browserID, ep);
      }

      connectionEndpoint.getMessagingServer().getRemotingService()
            .getDispatcher().register(ep.newHandler());

      log.trace(this + " created and registered " + ep);

      return new SessionCreateBrowserResponseMessage(browserID);
   }

   private void checkSecurityCreateConsumerDelegate(String address,
         String subscriptionName) throws Exception
   {
      security.check(address, CheckType.READ, this.getConnectionEndpoint());
   }

   public PacketHandler newHandler()
   {
      return new SessionAdvisedPacketHandler();
   }

   // Inner classes
   // --------------------------------------------------------------------------------

   private class SessionAdvisedPacketHandler extends ServerPacketHandlerSupport
   {
      public SessionAdvisedPacketHandler()
      {
      }

      public String getID()
      {
         return ServerSessionEndpoint.this.id;
      }

      public Packet doHandle(Packet packet, PacketSender sender)
            throws Exception
      {
         Packet response = null;

         PacketType type = packet.getType();

         // TODO use a switch for this
         if (type == SESS_SEND)
         {
            SessionSendMessage message = (SessionSendMessage) packet;

            send(message.getAddress(), message.getMessage());
         }
         else if (type == SESS_CREATECONSUMER)
         {
            SessionCreateConsumerMessage request = (SessionCreateConsumerMessage) packet;

            response = createConsumer(request.getQueueName(), request
                  .getFilterString(), request.isNoLocal(), request.isAutoDeleteQueue());
         }
         else if (type == SESS_CREATEQUEUE)
         {
            SessionCreateQueueMessage request = (SessionCreateQueueMessage) packet;

            createQueue(request.getAddress(), request.getQueueName(), request
                  .getFilterString(), request.isDurable(), request
                  .isTemporary());
         }
         else if (type == SESS_DELETE_QUEUE)
         {
            SessionDeleteQueueMessage request = (SessionDeleteQueueMessage) packet;

            deleteQueue(request.getQueueName());
         }
         else if (type == SESS_QUEUEQUERY)
         {
            SessionQueueQueryMessage request = (SessionQueueQueryMessage) packet;

            response = executeQueueQuery(request);
         }
         else if (type == SESS_BINDINGQUERY)
         {
            SessionBindingQueryMessage request = (SessionBindingQueryMessage)packet;
            
            response = executeBindingQuery(request);
         }
         else if (type == SESS_CREATEBROWSER)
         {
            SessionCreateBrowserMessage request = (SessionCreateBrowserMessage) packet;

            response = createBrowser(request.getQueueName(), request
                  .getFilterString());
         }
         else if (type == CLOSE)
         {
            close();
         }
         else if (type == SESS_ACKNOWLEDGE)
         {
            SessionAcknowledgeMessage message = (SessionAcknowledgeMessage) packet;

            acknowledge(message.getDeliveryID(), message.isAllUpTo());
         }
         else if (type == SESS_COMMIT)
         {
            commit();
         }
         else if (type == SESS_ROLLBACK)
         {
            rollback();
         }
         else if (type == SESS_CANCEL)
         {
            SessionCancelMessage message = (SessionCancelMessage) packet;

            cancel(message.getDeliveryID(), message.isExpired());
         }
         else if (type == SESS_XA_COMMIT)
         {
            SessionXACommitMessage message = (SessionXACommitMessage) packet;

            response = XACommit(message.isOnePhase(), message.getXid());
         }
         else if (type == SESS_XA_END)
         {
            SessionXAEndMessage message = (SessionXAEndMessage) packet;

            response = XAEnd(message.getXid(), message.isFailed());
         }
         else if (type == SESS_XA_FORGET)
         {
            SessionXAForgetMessage message = (SessionXAForgetMessage) packet;

            response = XAForget(message.getXid());
         }
         else if (type == SESS_XA_JOIN)
         {
            SessionXAJoinMessage message = (SessionXAJoinMessage) packet;

            response = XAJoin(message.getXid());
         }
         else if (type == SESS_XA_RESUME)
         {
            SessionXAResumeMessage message = (SessionXAResumeMessage) packet;

            response = XAResume(message.getXid());
         }
         else if (type == SESS_XA_ROLLBACK)
         {
            SessionXARollbackMessage message = (SessionXARollbackMessage) packet;

            response = XARollback(message.getXid());
         }
         else if (type == SESS_XA_START)
         {
            SessionXAStartMessage message = (SessionXAStartMessage) packet;

            response = XAStart(message.getXid());
         }
         else if (type == SESS_XA_SUSPEND)
         {
            response = XASuspend();
         }
         else if (type == SESS_XA_PREPARE)
         {
            SessionXAPrepareMessage message = (SessionXAPrepareMessage) packet;

            response = XAPrepare(message.getXid());
         }
         else if (type == SESS_XA_INDOUBT_XIDS)
         {
            List<Xid> xids = getInDoubtXids();

            response = new SessionXAGetInDoubtXidsResponseMessage(xids);
         }
         else if (type == SESS_XA_GET_TIMEOUT)
         {
            response = new SessionXAGetTimeoutResponseMessage(getXATimeout());
         }
         else if (type == SESS_XA_SET_TIMEOUT)
         {
            SessionXASetTimeoutMessage message = (SessionXASetTimeoutMessage) packet;

            response = new SessionXASetTimeoutResponseMessage(setXATimeout(message
                  .getTimeoutSeconds()));
         }
         else if (type == PacketType.SESS_ADD_ADDRESS)
         {
            SessionAddAddressMessage message = (SessionAddAddressMessage) packet;

            addAddress(message.getAddress());
         }
         else if (type == PacketType.SESS_REMOVE_ADDRESS)
         {
            SessionRemoveAddressMessage message = (SessionRemoveAddressMessage) packet;

            removeAddress(message.getAddress());
         }
         else
         {
            throw new MessagingException(MessagingException.UNSUPPORTED_PACKET,
                  "Unsupported packet " + type);
         }

         // reply if necessary
         if (response == null && packet.isOneWay() == false)
         {
            response = new NullPacket();
         }

         return response;
      }

      @Override
      public String toString()
      {
         return "SessionAdvisedPacketHandler[id=" + id + "]";
      }
   }

}
