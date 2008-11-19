/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.DelayedResult;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.NullResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionExpiredMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReplicateDeliveryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendChunkMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendMessage;
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
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.SendLock;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerLargeMessage;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.IDGenerator;
import org.jboss.messaging.util.SimpleIDGenerator;
import org.jboss.messaging.util.SimpleString;

/*
 * Session implementation 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a> 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 */
public class ServerSessionImpl implements ServerSession, FailureListener, NotificationListener
{
   // Constants -----------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionImpl.class);

   // Static -------------------------------------------------------------------------------

   public static void moveReferencesBackToHeadOfQueues(List<MessageReference> references,
                                                       PostOffice postOffice,
                                                       StorageManager storageManager,
                                                       HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      Map<Queue, LinkedList<MessageReference>> queueMap = new HashMap<Queue, LinkedList<MessageReference>>();

      for (MessageReference ref : references)
      {
         if (ref.cancel(storageManager, postOffice, queueSettingsRepository))
         {
            Queue queue = ref.getQueue();

            LinkedList<MessageReference> list = queueMap.get(queue);

            if (list == null)
            {
               list = new LinkedList<MessageReference>();

               queueMap.put(queue, list);
            }

            list.add(ref);
         }
      }

      for (Map.Entry<Queue, LinkedList<MessageReference>> entry : queueMap.entrySet())
      {
         LinkedList<MessageReference> refs = entry.getValue();

         entry.getKey().addListFirst(refs);
      }
   }

   // Attributes ----------------------------------------------------------------------------

   private final boolean trace = log.isTraceEnabled();

   private final long id;

   private final String username;

   private final String password;

   private final int minLargeMessageSize;

   private final boolean autoCommitSends;

   private final boolean autoCommitAcks;

   private volatile RemotingConnection remotingConnection;

   private final Map<Long, ServerConsumer> consumers = new ConcurrentHashMap<Long, ServerConsumer>();

   private final Executor executor;

   private Transaction tx;

   private final StorageManager storageManager;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final ResourceManager resourceManager;

   private final PostOffice postOffice;

   private final PagingManager pager;

   private final SecurityStore securityStore;

   private final Channel channel;

   private final ManagementService managementService;

   private volatile boolean started = false;

   private final List<Runnable> failureRunners = new ArrayList<Runnable>();

   private final IDGenerator idGenerator = new SimpleIDGenerator(0);

   private final String name;

   private final MessagingServer server;

   private final SimpleString managementAddress;

   private ServerLargeMessage largeMessage;

   // Constructors ---------------------------------------------------------------------------------

   public ServerSessionImpl(final String name,
                            final long id,
                            final String username,
                            final String password,
                            final int minLargeMessageSize,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean xa,
                            final RemotingConnection remotingConnection,
                            final StorageManager storageManager,
                            final PostOffice postOffice,
                            final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                            final ResourceManager resourceManager,
                            final SecurityStore securityStore,
                            final Executor executor,
                            final Channel channel,
                            final ManagementService managementService,
                            final MessagingServer server,
                            final SimpleString managementAddress) throws Exception
   {
      this.id = id;

      this.username = username;

      this.password = password;

      this.minLargeMessageSize = minLargeMessageSize;

      this.autoCommitSends = autoCommitSends;

      this.autoCommitAcks = autoCommitAcks;

      this.remotingConnection = remotingConnection;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.pager = postOffice.getPagingManager();

      this.queueSettingsRepository = queueSettingsRepository;

      this.resourceManager = resourceManager;

      this.securityStore = securityStore;

      this.executor = executor;

      if (!xa)
      {
         tx = new TransactionImpl(storageManager, postOffice);
      }

      this.channel = channel;

      this.managementService = managementService;

      this.name = name;

      this.server = server;

      this.managementAddress = managementAddress;
   }

   // ServerSession implementation ----------------------------------------------------------------------------

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public long getID()
   {
      return id;
   }

   public void removeConsumer(final ServerConsumer consumer) throws Exception
   {
      if (consumers.remove(consumer.getID()) == null)
      {
         throw new IllegalStateException("Cannot find consumer with id " + consumer.getID() + " to remove");
      }
   }

   public void close() throws Exception
   {
      rollback();

      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.close();
      }

      consumers.clear();

      server.removeSession(name);

      if (largeMessage != null)
      {
         try
         {
            largeMessage.deleteFile();
         }
         catch (Throwable error)
         {
            log.warn(error.toString(), error);

         }
      }

   }

   public void promptDelivery(final Queue queue)
   {
      queue.deliverAsync(executor);
   }

   public void doHandleCreateConsumer(final SessionCreateConsumerMessage packet)
   {
      SimpleString queueName = packet.getQueueName();

      SimpleString filterString = packet.getFilterString();

      boolean browseOnly = packet.isBrowseOnly();

      Packet response = null;

      try
      {
         Binding binding = postOffice.getBinding(queueName);

         if (binding == null)
         {
            throw new MessagingException(MessagingException.QUEUE_DOES_NOT_EXIST);
         }

         securityStore.check(binding.getAddress(), CheckType.READ, this);

         Filter filter = null;

         if (filterString != null)
         {
            filter = new FilterImpl(filterString);
         }

         Queue theQueue;
         if (browseOnly)
         {
            // We consume a copy of the queue - TODO - this is a temporary measure
            // and will disappear once we can provide a proper iterator on the queue

            theQueue = new QueueImpl(-1, queueName, filter, false, false, false, null, postOffice);

            // There's no need for any special locking since the list method is synchronized
            List<MessageReference> refs = binding.getQueue().list(filter);

            for (MessageReference ref : refs)
            {
               theQueue.addLast(ref);
            }
         }
         else
         {
            theQueue = binding.getQueue();
         }

         ServerConsumer consumer = new ServerConsumerImpl(idGenerator.generateID(),
                                                          this,
                                                          theQueue,
                                                          filter,
                                                          started,
                                                          browseOnly,
                                                          storageManager,
                                                          queueSettingsRepository,
                                                          postOffice,
                                                          channel,
                                                          pager);

         consumers.put(consumer.getID(), consumer);

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to create consumer", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleCreateConsumer(final SessionCreateConsumerMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleCreateConsumer(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleCreateConsumer(packet);
            }
         });
      }
   }

   public void doHandleCreateQueue(final SessionCreateQueueMessage packet)
   {
      SimpleString address = packet.getAddress();

      SimpleString queueName = packet.getQueueName();

      SimpleString filterString = packet.getFilterString();

      boolean temporary = packet.isTemporary();

      boolean durable = packet.isDurable();

      boolean fanout = packet.isFanout();

      Packet response = null;

      try
      {
         // make sure the user has privileges to create this queue
         if (!postOffice.containsDestination(address))
         {
            securityStore.check(address, CheckType.CREATE, this);
         }

         Binding binding = postOffice.getBinding(queueName);

         if (binding != null)
         {
            throw new MessagingException(MessagingException.QUEUE_EXISTS);
         }

         Filter filter = null;

         if (filterString != null)
         {
            filter = new FilterImpl(filterString);
         }

         binding = postOffice.addBinding(address, queueName, filter, durable, temporary, fanout);

         if (temporary)
         {
            // Temporary queue in core simply means the queue will be deleted if
            // the remoting connection
            // dies. It does not mean it will get deleted automatically when the
            // session is closed.
            // It is up to the user to delete the queue when finished with it

            final Queue queue = binding.getQueue();

            failureRunners.add(new Runnable()
            {
               public void run()
               {
                  try
                  {
                     postOffice.removeBinding(queue.getName());
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to remove temporary queue " + queue.getName());
                  }
               }
            });
         }

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to create queue", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleCreateQueue(final SessionCreateQueueMessage packet)
   {
      final SendLock lock;
      if (channel.getReplicatingChannel() != null)
      {
         lock = postOffice.getAddressLock(packet.getAddress());

         lock.lock();
      }
      else
      {
         lock = null;
      }

      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleCreateQueue(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleCreateQueue(packet);

               lock.unlock();
            }
         });
      }
   }

   public void handleDeleteQueue(final SessionDeleteQueueMessage packet)
   {
      final SendLock lock;
      if (channel.getReplicatingChannel() != null)
      {
         Binding binding = postOffice.getBinding(packet.getQueueName());
         lock = postOffice.getAddressLock(binding.getAddress());

         lock.lock();
      }
      else
      {
         lock = null;
      }

      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleDeleteQueue(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleDeleteQueue(packet);

               lock.unlock();
            }
         });
      }
   }

   public void doHandleDeleteQueue(final SessionDeleteQueueMessage packet)
   {
      SimpleString queueName = packet.getQueueName();

      Packet response = null;

      try
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
            binding.getQueue().deleteAllReferences(storageManager);
         }

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to delete consumer", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleExecuteQueueQuery(final SessionQueueQueryMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleExecuteQueueQuery(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleExecuteQueueQuery(packet);
            }
         });
      }
   }

   public void doHandleExecuteQueueQuery(final SessionQueueQueryMessage packet)
   {
      SimpleString queueName = packet.getQueueName();

      Packet response = null;

      try
      {
         if (queueName == null)
         {
            throw new IllegalArgumentException("Queue name is null");
         }

         Binding binding = postOffice.getBinding(queueName);

         if (binding != null)
         {
            Queue queue = binding.getQueue();

            Filter filter = queue.getFilter();

            SimpleString filterString = filter == null ? null : filter.getFilterString();
            // TODO: Remove MAX-SIZE-BYTES from SessionQueueQueryResponse.
            response = new SessionQueueQueryResponseMessage(queue.isDurable(),
                                                            queue.getConsumerCount(),
                                                            queue.getMessageCount(),
                                                            filterString,
                                                            binding.getAddress());
         }
         else
         {
            response = new SessionQueueQueryResponseMessage();
         }
      }
      catch (Exception e)
      {
         log.error("Failed to execute queue query", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleExecuteBindingQuery(final SessionBindingQueryMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleExecuteBindingQuery(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleExecuteBindingQuery(packet);
            }
         });
      }
   }

   public void doHandleExecuteBindingQuery(final SessionBindingQueryMessage packet)
   {
      SimpleString address = packet.getAddress();

      Packet response = null;

      try
      {
         if (address == null)
         {
            throw new IllegalArgumentException("Address is null");
         }

         boolean exists = postOffice.containsDestination(address);

         List<SimpleString> queueNames = new ArrayList<SimpleString>();

         if (exists)
         {
            List<Binding> bindings = postOffice.getBindingsForAddress(address);

            for (Binding binding : bindings)
            {
               queueNames.add(binding.getQueue().getName());
            }
         }

         response = new SessionBindingQueryResponseMessage(exists, queueNames);
      }
      catch (Exception e)
      {
         log.error("Failed to execute binding query", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleAcknowledge(final SessionAcknowledgeMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleAcknowledge(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleAcknowledge(packet);
            }
         });
      }
   }

   public void doHandleAcknowledge(final SessionAcknowledgeMessage packet)
   {
      Packet response = null;

      try
      {
         ServerConsumer consumer = consumers.get(packet.getConsumerID());

         consumer.acknowledge(autoCommitAcks, tx, packet.getMessageID());

         if (packet.isRequiresResponse())
         {
            response = new NullResponseMessage();
         }
      }
      catch (Exception e)
      {
         log.error("Failed to acknowledge", e);

         if (packet.isRequiresResponse())
         {
            if (e instanceof MessagingException)
            {
               response = new MessagingExceptionMessage((MessagingException)e);
            }
            else
            {
               response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
            }
         }
      }

      channel.confirm(packet);

      if (response != null)
      {
         channel.send(response);
      }
   }

   public void handleExpired(final SessionExpiredMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleExpired(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleExpired(packet);
            }
         });
      }
   }

   public void doHandleExpired(final SessionExpiredMessage packet)
   {
      try
      {
         MessageReference ref = consumers.get(packet.getConsumerID()).getExpired(packet.getMessageID());

         // Null implies a browser
         if (ref != null)
         {
            ref.expire(storageManager, postOffice, queueSettingsRepository);
         }
      }
      catch (Exception e)
      {
         log.error("Failed to acknowledge", e);
      }

      channel.confirm(packet);
   }

   public void handleCommit(final Packet packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleCommit(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleCommit(packet);
            }
         });
      }
   }

   public void doHandleCommit(final Packet packet)
   {
      Packet response = null;

      try
      {
         tx.commit();

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to commit", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }
      finally
      {
         tx = new TransactionImpl(storageManager, postOffice);
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleRollback(final Packet packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleRollback(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleRollback(packet);
            }
         });
      }
   }

   public void doHandleRollback(final Packet packet)
   {
      Packet response = null;

      try
      {
         rollback();

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to rollback", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleXACommit(final SessionXACommitMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleXACommit(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleXACommit(packet);
            }
         });
      }
   }

   public void doHandleXACommit(final SessionXACommitMessage packet)
   {
      Packet response = null;

      Xid xid = packet.getXid();

      try
      {
         if (tx != null)
         {
            final String msg = "Cannot commit, session is currently doing work in transaction " + tx.getXid();

            response = new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
         }
         else
         {
            Transaction theTx = resourceManager.removeTransaction(xid);

            if (theTx == null)
            {
               final String msg = "Cannot find xid in resource manager: " + xid;

               response = new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
            }
            else
            {
               if (theTx.getState() == Transaction.State.SUSPENDED)
               {
                  // Put it back
                  resourceManager.putTransaction(xid, tx);

                  response = new SessionXAResponseMessage(true,
                                                          XAException.XAER_PROTO,
                                                          "Cannot commit transaction, it is suspended " + xid);
               }
               else
               {
                  theTx.commit();

                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
               }
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa commit", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleXAEnd(final SessionXAEndMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleXAEnd(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleXAEnd(packet);
            }
         });
      }
   }

   public void doHandleXAEnd(final SessionXAEndMessage packet)
   {
      Packet response = null;

      Xid xid = packet.getXid();

      try
      {
         if (tx != null && tx.getXid().equals(xid))
         {
            if (tx.getState() == Transaction.State.SUSPENDED)
            {
               final String msg = "Cannot end, transaction is suspended";

               response = new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
            }
            else
            {
               tx = null;
            }
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
               final String msg = "Cannot find suspended transaction to end " + xid;

               response = new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
            }
            else
            {
               if (theTx.getState() != Transaction.State.SUSPENDED)
               {
                  final String msg = "Transaction is not suspended " + xid;

                  response = new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
               }
               else
               {
                  theTx.resume();
               }
            }
         }

         if (response == null)
         {
            response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa end", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleXAForget(final SessionXAForgetMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleXAForget(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleXAForget(packet);
            }
         });
      }
   }

   public void doHandleXAForget(final SessionXAForgetMessage packet)
   {
      // Do nothing since we don't support heuristic commits / rollback from the
      // resource manager

      Packet response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleXAJoin(final SessionXAJoinMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleXAJoin(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleXAJoin(packet);
            }
         });
      }
   }

   public void doHandleXAJoin(final SessionXAJoinMessage packet)
   {
      Packet response = null;

      Xid xid = packet.getXid();

      try
      {
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;

            response = new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               response = new SessionXAResponseMessage(true,
                                                       XAException.XAER_PROTO,
                                                       "Cannot join tx, it is suspended " + xid);
            }
            else
            {
               tx = theTx;

               response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa join", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleXAResume(final SessionXAResumeMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleXAResume(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleXAResume(packet);
            }
         });
      }
   }

   public void doHandleXAResume(final SessionXAResumeMessage packet)
   {
      Packet response = null;

      Xid xid = packet.getXid();

      try
      {
         if (tx != null)
         {
            final String msg = "Cannot resume, session is currently doing work in a transaction " + tx.getXid();

            response = new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
         }
         else
         {
            Transaction theTx = resourceManager.getTransaction(xid);

            if (theTx == null)
            {
               final String msg = "Cannot find xid in resource manager: " + xid;

               response = new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
            }
            else
            {
               if (theTx.getState() != Transaction.State.SUSPENDED)
               {
                  response = new SessionXAResponseMessage(true,
                                                          XAException.XAER_PROTO,
                                                          "Cannot resume transaction, it is not suspended " + xid);
               }
               else
               {
                  tx = theTx;

                  tx.resume();

                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
               }
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa resume", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleXARollback(final SessionXARollbackMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleXARollback(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleXARollback(packet);
            }
         });
      }
   }

   public void doHandleXARollback(final SessionXARollbackMessage packet)
   {
      Packet response = null;

      Xid xid = packet.getXid();

      try
      {
         if (tx != null)
         {
            final String msg = "Cannot roll back, session is currently doing work in a transaction " + tx.getXid();

            response = new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
         }
         else
         {
            Transaction theTx = resourceManager.removeTransaction(xid);

            if (theTx == null)
            {
               final String msg = "Cannot find xid in resource manager: " + xid;

               response = new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
            }
            else
            {
               if (theTx.getState() == Transaction.State.SUSPENDED)
               {
                  // Put it back
                  resourceManager.putTransaction(xid, tx);

                  response = new SessionXAResponseMessage(true,
                                                          XAException.XAER_PROTO,
                                                          "Cannot rollback transaction, it is suspended " + xid);
               }
               else
               {
                  doRollback(theTx);

                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
               }
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa rollback", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleXAStart(final SessionXAStartMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleXAStart(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleXAStart(packet);
            }
         });
      }
   }

   public void doHandleXAStart(final SessionXAStartMessage packet)
   {
      Packet response = null;

      Xid xid = packet.getXid();

      try
      {
         if (tx != null)
         {
            final String msg = "Cannot start, session is already doing work in a transaction " + tx.getXid();

            response = new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
         }
         else
         {
            tx = new TransactionImpl(xid, storageManager, postOffice);

            boolean added = resourceManager.putTransaction(xid, tx);

            if (!added)
            {
               final String msg = "Cannot start, there is already a xid " + tx.getXid();

               response = new SessionXAResponseMessage(true, XAException.XAER_DUPID, msg);
            }
            else
            {
               response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa start", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleXASuspend(final Packet packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleXASuspend(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleXASuspend(packet);
            }
         });
      }
   }

   public void doHandleXASuspend(final Packet packet)
   {
      Packet response = null;

      try
      {
         if (tx == null)
         {
            final String msg = "Cannot suspend, session is not doing work in a transaction ";

            response = new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
         }
         else
         {
            if (tx.getState() == Transaction.State.SUSPENDED)
            {
               final String msg = "Cannot suspend, transaction is already suspended " + tx.getXid();

               response = new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
            }
            else
            {
               tx.suspend();

               tx = null;

               response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa suspend", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleXAPrepare(final SessionXAPrepareMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleXAPrepare(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleXAPrepare(packet);
            }
         });
      }
   }

   public void doHandleXAPrepare(final SessionXAPrepareMessage packet)
   {
      Packet response = null;

      Xid xid = packet.getXid();

      try
      {
         if (tx != null)
         {
            final String msg = "Cannot commit, session is currently doing work in a transaction " + tx.getXid();

            response = new SessionXAResponseMessage(true, XAException.XAER_PROTO, msg);
         }
         else
         {
            Transaction theTx = resourceManager.getTransaction(xid);

            if (theTx == null)
            {
               final String msg = "Cannot find xid in resource manager: " + xid;

               response = new SessionXAResponseMessage(true, XAException.XAER_NOTA, msg);
            }
            else
            {
               if (theTx.getState() == Transaction.State.SUSPENDED)
               {
                  response = new SessionXAResponseMessage(true,
                                                          XAException.XAER_PROTO,
                                                          "Cannot prepare transaction, it is suspended " + xid);
               }
               else
               {
                  if (theTx.isEmpty())
                  {
                     response = new SessionXAResponseMessage(false, XAResource.XA_RDONLY, null);
                  }
                  else
                  {
                     theTx.prepare();

                     response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
                  }
               }
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa prepare", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleGetInDoubtXids(final Packet packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleGetInDoubtXids(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleGetInDoubtXids(packet);
            }
         });
      }
   }

   public void doHandleGetInDoubtXids(final Packet packet)
   {
      Packet response = new SessionXAGetInDoubtXidsResponseMessage(resourceManager.getPreparedTransactions());

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleGetXATimeout(final Packet packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleGetXATimeout(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleGetXATimeout(packet);
            }
         });
      }
   }

   public void doHandleGetXATimeout(final Packet packet)
   {
      Packet response = new SessionXAGetTimeoutResponseMessage(resourceManager.getTimeoutSeconds());

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleSetXATimeout(final SessionXASetTimeoutMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleSetXATimeout(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleSetXATimeout(packet);
            }
         });
      }
   }

   public void doHandleSetXATimeout(final SessionXASetTimeoutMessage packet)
   {
      Packet response = new SessionXASetTimeoutResponseMessage(resourceManager.setTimeoutSeconds(packet.getTimeoutSeconds()));

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleAddDestination(final SessionAddDestinationMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleAddDestination(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleAddDestination(packet);
            }
         });
      }
   }

   public void doHandleAddDestination(final SessionAddDestinationMessage packet)
   {
      Packet response = null;

      final SimpleString address = packet.getAddress();

      final boolean durable = packet.isDurable();

      final boolean temporary = packet.isTemporary();

      try
      {
         securityStore.check(address, CheckType.CREATE, this);

         if (!postOffice.addDestination(address, durable))
         {
            throw new MessagingException(MessagingException.ADDRESS_EXISTS, "Address already exists: " + address);
         }

         if (temporary)
         {
            // Temporary address in core simply means the address will be deleted
            // if the remoting connection
            // dies. It does not mean it will get deleted automatically when the
            // session is closed.
            // It is up to the user to delete the address when finished with it

            failureRunners.add(new Runnable()
            {
               public void run()
               {
                  try
                  {
                     postOffice.removeDestination(address, durable);
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to remove temporary address " + address);
                  }
               }
            });
         }

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to add destination", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void handleRemoveDestination(final SessionRemoveDestinationMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleRemoveDestination(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleRemoveDestination(packet);
            }
         });
      }
   }

   public void doHandleRemoveDestination(final SessionRemoveDestinationMessage packet)
   {
      Packet response = null;

      final SimpleString address = packet.getAddress();

      final boolean durable = packet.isDurable();

      try
      {
         securityStore.check(address, CheckType.CREATE, this);

         if (!postOffice.removeDestination(address, durable))
         {
            throw new MessagingException(MessagingException.ADDRESS_DOES_NOT_EXIST,
                                         "Address does not exist: " + address);
         }

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to remove destination", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   private void lockConsumers()
   {
      for (ServerConsumer consumer : consumers.values())
      {
         consumer.lock();
      }
   }

   private void unlockConsumers()
   {
      for (ServerConsumer consumer : consumers.values())
      {
         consumer.unlock();
      }
   }

   public void handleStart(final Packet packet)
   {
      boolean lock = this.channel.getReplicatingChannel() != null;

      if (lock)
      {
         lockConsumers();
      }

      // We need to prevent any delivery and replication of delivery occurring while the start/stop
      // is being processed.
      // Otherwise we can end up with start/stop being processed in different order on backup to live.
      // Which can result in, say, a delivery arriving at backup, but it's still not started!
      DelayedResult result = null;
      try
      {
         result = channel.replicatePacket(packet);

         // note we process start before response is back from the backup
         setStarted(true);
      }
      finally
      {
         if (lock)
         {
            unlockConsumers();
         }
      }

      if (result == null)
      {
         channel.confirm(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               channel.confirm(packet);
            }
         });
      }
   }

   // TODO try removing the lock consumers and see what happens!!
   public void handleStop(final Packet packet)
   {
      boolean lock = channel.getReplicatingChannel() != null;

      if (lock)
      {
         lockConsumers();
      }

      try
      {
         DelayedResult result = channel.replicatePacket(packet);

         // note we process stop before response is back from the backup

         final Packet response = new NullResponseMessage();

         setStarted(false);

         if (result == null)
         {
            channel.confirm(packet);
            // Not clustered - just send now
            channel.send(response);
         }
         else
         {
            result.setResultRunner(new Runnable()
            {
               public void run()
               {
                  channel.confirm(packet);

                  channel.send(response);
               }
            });
         }
      }
      finally
      {
         if (lock)
         {
            unlockConsumers();
         }
      }
   }

   public void handleFailedOver(final Packet packet)
   {
      // No need to replicate since this only occurs after failover
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.failedOver();
      }
   }

   public void handleClose(final Packet packet)
   {
      // We need to stop the consumers first before replicating, to ensure no deliveries occur after this,
      // but we need to process the actual close() when the replication response returns, otherwise things
      // can happen like acks can come in after close

      for (ServerConsumer consumer : consumers.values())
      {
         consumer.setStarted(false);
      }

      DelayedResult result = channel.replicatePacket(packet);

      if (result == null)
      {
         doHandleClose(packet);
      }
      else
      {
         // Don't process until result has come back from backup
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleClose(packet);
            }
         });
      }
   }

   public void doHandleClose(final Packet packet)
   {
      Packet response = null;

      try
      {
         close();

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to close", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);

      channel.close();
   }

   private void setStarted(final boolean s)
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.setStarted(s);
      }

      started = s;
   }

   public void handleCloseConsumer(final SessionConsumerCloseMessage packet)
   {
      // We need to stop the consumer first before replicating, to ensure no deliveries occur after this,
      // but we need to process the actual close() when the replication response returns, otherwise things
      // can happen like acks can come in after close

      ServerConsumer consumer = consumers.get(packet.getConsumerID());

      consumer.handleClose(packet);
   }

   public void handleReceiveConsumerCredits(final SessionConsumerFlowCreditMessage packet)
   {
      DelayedResult result = channel.replicatePacket(packet);

      try
      {
         // Note we don't wait for response before handling this

         consumers.get(packet.getConsumerID()).receiveCredits(packet.getCredits());
      }
      catch (Exception e)
      {
         log.error("Failed to receive credits", e);
      }

      if (result == null)
      {
         channel.confirm(packet);
      }
      else
      {
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               channel.confirm(packet);
            }
         });
      }
   }

   public void handleSendChunkMessage(final SessionSendChunkMessage packet)
   {

      if (packet.getMessageID() == 0)
      {
         packet.setMessageID(storageManager.generateUniqueID());
      }

      Packet response = null;

      // TODO: Replication on ChunkMessages

      try
      {
         if (packet.getHeader() != null)
         {
            largeMessage = createLargeMessageStorage(packet.getTargetID(), packet.getMessageID(), packet.getHeader());
         }

         largeMessage.addBytes(packet.getBody());

         if (!packet.isContinues())
         {
            final ServerLargeMessage message = largeMessage;
            largeMessage = null;

            message.complete();
            send(message);
         }

         if (packet.isRequiresResponse())
         {
            response = new NullResponseMessage();
         }

      }
      catch (Exception e)
      {
         log.error("Failed to send message", e);

         if (packet.isRequiresResponse())
         {
            if (e instanceof MessagingException)
            {
               response = new MessagingExceptionMessage((MessagingException)e);
            }
            else
            {
               response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
            }
         }
      }

      channel.confirm(packet);

      if (response != null)
      {
         channel.send(response);
      }

   }

   public void handleSend(final SessionSendMessage packet)
   {
      // With a send we must make sure it is replicated to backup before being processed on live
      // or can end up with delivery being processed on backup before original send

      ServerMessage msg = packet.getServerMessage();

      final SendLock lock;

      if (channel.getReplicatingChannel() != null)
      {
         lock = postOffice.getAddressLock(msg.getDestination());

         lock.beforeSend();
      }
      else
      {
         lock = null;
      }

      if (msg.getMessageID() == 0L)
      {
         // must generate message id here, so we know they are in sync on live and backup
         long id = storageManager.generateUniqueID();

         msg.setMessageID(id);
      }

      DelayedResult result = channel.replicatePacket(packet);

      // With a send we must make sure it is replicated to backup before being processed on live
      // or can end up with delivery being processed on backup before original send

      if (result == null)
      {
         doSend(packet);
      }
      else
      {
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doSend(packet);

               lock.afterSend();
            }
         });
      }
   }

   private void doSend(final SessionSendMessage packet)
   {
      Packet response = null;

      try
      {
         ServerMessage message = packet.getServerMessage();

         if (message.getDestination().equals(managementAddress))
         {
            // It's a management message

            handleManagementMessage(message);
         }
         else
         {
            send(message);
         }

         if (packet.isRequiresResponse())
         {
            response = new NullResponseMessage();
         }
      }
      catch (Exception e)
      {
         log.error("Failed to send message", e);

         if (packet.isRequiresResponse())
         {
            if (e instanceof MessagingException)
            {
               response = new MessagingExceptionMessage((MessagingException)e);
            }
            else
            {
               response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
            }
         }
      }

      channel.confirm(packet);

      if (response != null)
      {
         channel.send(response);
      }
   }

   private void handleManagementMessage(final ServerMessage message) throws Exception
   {
      doSecurity(message);

      if (message.containsProperty(ManagementHelper.HDR_JMX_SUBSCRIBE_TO_NOTIFICATIONS))
      {
         boolean subscribe = (Boolean)message.getProperty(ManagementHelper.HDR_JMX_SUBSCRIBE_TO_NOTIFICATIONS);

         final SimpleString replyTo = (SimpleString)message.getProperty(ManagementHelper.HDR_JMX_REPLYTO);

         if (subscribe)
         {
            if (log.isDebugEnabled())
            {
               log.debug("added notification listener " + this);
            }

            managementService.addNotificationListener(this, null, replyTo);
         }
         else
         {
            if (log.isDebugEnabled())
            {
               log.debug("removed notification listener " + this);
            }

            managementService.removeNotificationListener(this);
         }
      }
      else
      {
         managementService.handleMessage(message);

         message.setDestination((SimpleString)message.getProperty(ManagementHelper.HDR_JMX_REPLYTO));

         send(message);
      }
   }

   public void handleReplicatedDelivery(final SessionReplicateDeliveryMessage packet)
   {
      ServerConsumer consumer = consumers.get(packet.getConsumerID());

      if (consumer == null)
      {
         throw new IllegalStateException("Cannot handle replicated delivery, consumer is closed");
      }

      try
      {
         consumer.deliverReplicated(packet.getMessageID());
      }
      catch (Exception e)
      {
         log.error("Failed to handle replicated delivery", e);
      }
   }

   public int transferConnection(final RemotingConnection newConnection, final int lastReceivedCommandID)
   {
      remotingConnection.removeFailureListener(this);

      channel.transferConnection(newConnection);

      newConnection.syncIDGeneratorSequence(remotingConnection.getIDGeneratorSequence());

      // Destroy the old connection
      remotingConnection.destroy();

      remotingConnection = newConnection;

      remotingConnection.addFailureListener(this);

      int serverLastReceivedCommandID = channel.getLastReceivedCommandID();

      channel.replayCommands(lastReceivedCommandID);

      return serverLastReceivedCommandID;
   }

   public Channel getChannel()
   {
      return channel;
   }

   // FailureListener implementation
   // --------------------------------------------------------------------

   public void connectionFailed(final MessagingException me)
   {
      try
      {
         log.info("Connection failed, so clearing up resources for session " + name);
         for (Runnable runner : failureRunners)
         {
            try
            {
               runner.run();
            }
            catch (Throwable t)
            {
               log.error("Failed to execute failure runner", t);
            }
         }

         // We call handleClose() since we need to replicate the close too, if there is a backup
         handleClose(new PacketImpl(PacketImpl.SESS_CLOSE));

         log.info("Cleared up resources for session " + name);
      }
      catch (Throwable t)
      {
         log.error("Failed to close connection " + this);
      }
   }

   // NotificationListener implementation -------------------------------------

   public void handleNotification(final Notification notification, final Object replyTo)
   {
      // FIXME this won't work with replication
      ServerMessage notificationMessage = new ServerMessageImpl(storageManager.generateUniqueID());
      notificationMessage.setDestination((SimpleString)replyTo);
      notificationMessage.setBody(new ByteBufferWrapper(ByteBuffer.allocate(2048)));
      ManagementHelper.storeNotification(notificationMessage, notification);
      try
      {
         send(notificationMessage);
      }
      catch (Exception e)
      {
         log.warn("problem while sending a notification message " + notification, e);
      }
   }

   // Public
   // ----------------------------------------------------------------------------

   public Transaction getTransaction()
   {
      return tx;
   }

   // Private
   // ----------------------------------------------------------------------------

   private ServerLargeMessage createLargeMessageStorage(long producerID, long messageID, byte[] header) throws Exception
   {
      ServerLargeMessage largeMessage = storageManager.createLargeMessageStorage();

      MessagingBuffer headerBuffer = new ByteBufferWrapper(ByteBuffer.wrap(header));

      largeMessage.decodeProperties(headerBuffer);

      // client didn send the ID originally
      largeMessage.setMessageID(messageID);

      return largeMessage;
   }

   private void doRollback(final Transaction theTx) throws Exception
   {
      boolean wasStarted = started;

      List<MessageReference> toCancel = new ArrayList<MessageReference>();

      for (ServerConsumer consumer : consumers.values())
      {
         if (wasStarted)
         {
            consumer.setStarted(false);
         }

         toCancel.addAll(consumer.cancelRefs());
      }

      List<MessageReference> rolledBack = theTx.rollback(queueSettingsRepository);

      rolledBack.addAll(toCancel);

      if (wasStarted)
      {
         for (ServerConsumer consumer : consumers.values())
         {
            consumer.setStarted(true);
         }
      }

      // Now cancel the refs back to the queue(s), we sort into queues and cancel back atomically to
      // preserve order

      moveReferencesBackToHeadOfQueues(rolledBack, postOffice, storageManager, queueSettingsRepository);
   }

   private void rollback() throws Exception
   {
      if (tx == null)
      {
         // Might be null if XA

         tx = new TransactionImpl(storageManager, postOffice);
      }

      doRollback(tx);

      tx = new TransactionImpl(storageManager, postOffice);
   }

   private void send(final ServerMessage msg) throws Exception
   {
      // check the user has write access to this address.
      doSecurity(msg);

      Long scheduledDeliveryTime = (Long)msg.getProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME);

      if (autoCommitSends)
      {
         if (!pager.page(msg))
         {
            List<MessageReference> refs = postOffice.route(msg);

            if (msg.getDurableRefCount() != 0)
            {
               storageManager.storeMessage(msg);
            }

            for (MessageReference ref : refs)
            {
               if (scheduledDeliveryTime != null)
               {
                  ref.setScheduledDeliveryTime(scheduledDeliveryTime.longValue());

                  if (ref.getMessage().isDurable() && ref.getQueue().isDurable())
                  {
                     storageManager.updateScheduledDeliveryTime(ref);
                  }
               }

               ref.getQueue().addLast(ref);
            }
         }
      }
      else
      {
         tx.addMessage(msg);
      }
   }

   private void doSecurity(final ServerMessage msg) throws Exception
   {
      try
      {
         securityStore.check(msg.getDestination(), CheckType.WRITE, this);
      }
      catch (MessagingException e)
      {
         if (!autoCommitSends)
         {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }
   }

}
