/*
x * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.server.impl;

import static org.hornetq.core.management.NotificationType.CONSUMER_CREATED;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.Notification;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.CreateQueueMessage;
import org.hornetq.core.remoting.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.remoting.impl.wireformat.NullResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.RollbackMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionExpiredMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionForceConsumerDelivery;
import org.hornetq.core.remoting.impl.wireformat.SessionProducerCreditsMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionRequestProducerCreditsMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendLargeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerConsumer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/*
 * Session implementation 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a> 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 */
public class ServerSessionImpl implements ServerSession, FailureListener, CloseListener
{
   // Constants -----------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionImpl.class);

   // Static -------------------------------------------------------------------------------
   
   private static int offset;

   static
   {
      try
      {
         ServerMessage msg = new ServerMessageImpl();
         
         msg.setBody(ChannelBuffers.dynamicBuffer(0));
   
         msg.setDestination(new SimpleString("foobar"));
   
         int es = msg.getEncodeSize();
   
         int me = msg.getMemoryEstimate();
   
         offset = MessageReferenceImpl.getMemoryEstimate() + me - es;
      }
      catch (Exception e)
      {
         log.error("Failed to initialise mult and offset", e);
      }
   }

   // Attributes ----------------------------------------------------------------------------

   private final long id;

   private final String username;

   private final String password;

   private final int minLargeMessageSize;

   private final boolean autoCommitSends;

   private final boolean autoCommitAcks;

   private final boolean preAcknowledge;

   private final boolean updateDeliveries;

   private RemotingConnection remotingConnection;

   private final Map<Long, ServerConsumer> consumers = new ConcurrentHashMap<Long, ServerConsumer>();

   private final Executor executor;

   private Transaction tx;

   private final StorageManager storageManager;

   private final ResourceManager resourceManager;

   public final PostOffice postOffice;

   private final SecurityStore securityStore;

   private final Channel channel;

   private final ManagementService managementService;

   private volatile boolean started = false;

   private final List<Runnable> failureRunners = new ArrayList<Runnable>();

   private final String name;

   private final HornetQServer server;

   private final SimpleString managementAddress;
   
   // The current currentLargeMessage being processed
   private volatile LargeServerMessage currentLargeMessage;

   private ServerSessionPacketHandler handler;

   private boolean closed;

   private final Map<SimpleString, CreditManagerHolder> creditManagerHolders = new HashMap<SimpleString, CreditManagerHolder>();

   // Constructors ---------------------------------------------------------------------------------

   public ServerSessionImpl(final String name,
                            final String username,
                            final String password,
                            final int minLargeMessageSize,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean preAcknowledge,
                            final boolean updateDeliveries,
                            final boolean xa,
                            final RemotingConnection remotingConnection,
                            final StorageManager storageManager,
                            final PostOffice postOffice,
                            final ResourceManager resourceManager,
                            final SecurityStore securityStore,
                            final Executor executor,
                            final Channel channel,
                            final ManagementService managementService,
                            final HornetQServer server,
                            final SimpleString managementAddress) throws Exception
   {
      id = channel.getID();

      this.username = username;

      this.password = password;

      this.minLargeMessageSize = minLargeMessageSize;

      this.autoCommitSends = autoCommitSends;

      this.autoCommitAcks = autoCommitAcks;

      this.preAcknowledge = preAcknowledge;

      this.remotingConnection = remotingConnection;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.resourceManager = resourceManager;

      this.securityStore = securityStore;
      
      this.executor = executor;

      if (!xa)
      {
         tx = new TransactionImpl(storageManager);
      }

      this.updateDeliveries = updateDeliveries;

      this.channel = channel;

      this.managementService = managementService;

      this.name = name;

      this.server = server;

      this.managementAddress = managementAddress;

      remotingConnection.addFailureListener(this);

      remotingConnection.addCloseListener(this);
   }

   // ServerSession implementation ----------------------------------------------------------------------------

   public ServerSessionPacketHandler getHandler()
   {
      return handler;
   }

   public void setHandler(final ServerSessionPacketHandler handler)
   {
      this.handler = handler;
   }

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

   public String getName()
   {
      return name;
   }

   public Object getConnectionID()
   {
      return remotingConnection.getID();
   }

   public void removeConsumer(final ServerConsumer consumer) throws Exception
   {
      if (consumers.remove(consumer.getID()) == null)
      {
         throw new IllegalStateException("Cannot find consumer with id " + consumer.getID() + " to remove");
      }
   }

   public synchronized void close() throws Exception
   {
      if (tx != null && tx.getXid() == null)
      {
         // We only rollback local txs on close, not XA tx branches

         rollback(false);
      }

      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.close();
      }

      consumers.clear();

      server.removeSession(name);

      if (currentLargeMessage != null)
      {
         try
         {
            currentLargeMessage.deleteFile();
         }
         catch (Throwable error)
         {
            log.error("Failed to delete large message file", error);
         }
      }

      remotingConnection.removeFailureListener(this);

      // Return any outstanding credits

      closed = true;

      for (CreditManagerHolder holder : creditManagerHolders.values())
      {
         holder.store.returnProducerCredits(holder.outstandingCredits);
      }
   }

   public void promptDelivery(final Queue queue, final boolean async)
   {
      if (async)
      {
         queue.deliverAsync(executor);
      }
      else
      {
         queue.deliverNow();
      }
   }

   public void handleCreateConsumer(final SessionCreateConsumerMessage packet)
   {
      SimpleString name = packet.getQueueName();

      SimpleString filterString = packet.getFilterString();

      boolean browseOnly = packet.isBrowseOnly();

      Packet response = null;

      try
      {
         Binding binding = postOffice.getBinding(name);

         if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE)
         {
            throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST, "Queue " + name + " does not exist");
         }

         securityStore.check(binding.getAddress(), CheckType.CONSUME, this);

         Filter filter = FilterImpl.createFilter(filterString);;

         ServerConsumer consumer = new ServerConsumerImpl(packet.getID(),
                                                          this,
                                                          (QueueBinding)binding,
                                                          filter,
                                                          started,
                                                          browseOnly,
                                                          storageManager,
                                                          channel,
                                                          preAcknowledge,
                                                          updateDeliveries,
                                                          executor,
                                                          managementService);

         consumers.put(consumer.getID(), consumer);

         if (!browseOnly)
         {
            TypedProperties props = new TypedProperties();

            props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

            props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

            props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

            props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

            Queue theQueue = (Queue)binding.getBindable();

            props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, theQueue.getConsumerCount());

            if (filterString != null)
            {
               props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
            }

            Notification notification = new Notification(null, CONSUMER_CREATED, props);

            managementService.sendNotification(notification);
         }

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to create consumer", e);

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleCreateQueue(final CreateQueueMessage packet)
   {
      SimpleString address = packet.getAddress();

      final SimpleString name = packet.getQueueName();

      SimpleString filterString = packet.getFilterString();

      boolean temporary = packet.isTemporary();

      boolean durable = packet.isDurable();

      Packet response = null;

      try
      {
         if (durable)
         {
            // make sure the user has privileges to create this queue
            securityStore.check(address, CheckType.CREATE_DURABLE_QUEUE, this);
         }
         else
         {
            securityStore.check(address, CheckType.CREATE_NON_DURABLE_QUEUE, this);
         }

         server.createQueue(address, name, filterString, durable, temporary);

         if (temporary)
         {
            // Temporary queue in core simply means the queue will be deleted if
            // the remoting connection
            // dies. It does not mean it will get deleted automatically when the
            // session is closed.
            // It is up to the user to delete the queue when finished with it

            failureRunners.add(new Runnable()
            {
               public void run()
               {
                  try
                  {
                     if (postOffice.getBinding(name) != null)
                     {
                        postOffice.removeBinding(name);
                     }
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to remove temporary queue " + name);
                  }
               }
            });
         }

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            log.error("Failed to create queue", e);
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleDeleteQueue(final SessionDeleteQueueMessage packet)
   {
      SimpleString name = packet.getQueueName();

      Packet response = null;

      try
      {
         Binding binding = postOffice.getBinding(name);

         if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE)
         {
            throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST);
         }

         server.destroyQueue(name, this);

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to delete queue", e);

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleExecuteQueueQuery(final SessionQueueQueryMessage packet)
   {
      SimpleString name = packet.getQueueName();

      Packet response = null;

      try
      {
         if (name == null)
         {
            throw new IllegalArgumentException("Queue name is null");
         }

         Binding binding = postOffice.getBinding(name);

         if (binding != null && binding.getType() == BindingType.LOCAL_QUEUE)
         {
            Queue queue = (Queue)binding.getBindable();

            Filter filter = queue.getFilter();

            SimpleString filterString = filter == null ? null : filter.getFilterString();
            response = new SessionQueueQueryResponseMessage(queue.isDurable(),
                                                            queue.getConsumerCount(),
                                                            queue.getMessageCount(),
                                                            filterString,
                                                            binding.getAddress());
         }
         // make an exception for the management address (see HORNETQ-29)
         else if (name.equals(managementAddress))
         {
            response = new SessionQueueQueryResponseMessage(true, -1, -1, null, managementAddress);
         }
         else
         {
            response = new SessionQueueQueryResponseMessage();
         }
      }
      catch (Exception e)
      {
         log.error("Failed to execute queue query", e);

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleExecuteBindingQuery(final SessionBindingQueryMessage packet)
   {
      SimpleString address = packet.getAddress();

      Packet response = null;

      try
      {
         if (address == null)
         {
            throw new IllegalArgumentException("Address is null");
         }

         List<SimpleString> names = new ArrayList<SimpleString>();

         Bindings bindings = postOffice.getMatchingBindings(address);

         for (Binding binding : bindings.getBindings())
         {
            if (binding.getType() == BindingType.LOCAL_QUEUE)
            {
               names.add(binding.getUniqueName());
            }
         }

         response = new SessionBindingQueryResponseMessage(!names.isEmpty(), names);
      }
      catch (Exception e)
      {
         log.error("Failed to execute binding query", e);

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleForceConsumerDelivery(final SessionForceConsumerDelivery message)
   {
      try
      {
         ServerConsumer consumer = consumers.get(message.getConsumerID());

         consumer.forceDelivery(message.getSequence());
      }
      catch (Exception e)
      {
         log.error("Failed to query consumer deliveries", e);
      }

      sendResponse(message, null, false, false);
   }

   public void handleAcknowledge(final SessionAcknowledgeMessage packet)
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
            if (e instanceof HornetQException)
            {
               response = new HornetQExceptionMessage((HornetQException)e);
            }
            else
            {
               response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
            }
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleExpired(final SessionExpiredMessage packet)
   {
      try
      {
         MessageReference ref = consumers.get(packet.getConsumerID()).getExpired(packet.getMessageID());

         if (ref != null)
         {
            ref.getQueue().expire(ref);
         }
      }
      catch (Exception e)
      {
         log.error("Failed to acknowledge", e);
      }

      sendResponse(packet, null, false, false);
   }

   public void handleCommit(final Packet packet)
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

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }
      finally
      {
         tx = new TransactionImpl(storageManager);
      }

      sendResponse(packet, response, false, false);
   }

   public void handleRollback(final RollbackMessage packet)
   {
      Packet response = null;

      try
      {
         rollback(packet.isConsiderLastMessageAsDelivered());

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to rollback", e);

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleXACommit(final SessionXACommitMessage packet)
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
               // checked heuristic committed transactions
               if (resourceManager.getHeuristicCommittedTransactions().contains(xid))
               {
                  response = new SessionXAResponseMessage(true,
                                                          XAException.XA_HEURCOM,
                                                          "transaction has been heuristically committed: " + xid);
               }
               // checked heuristic rolled back transactions
               else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid))
               {
                  response = new SessionXAResponseMessage(true,
                                                          XAException.XA_HEURRB,
                                                          "transaction has been heuristically rolled back: " + xid);
               }
               else
               {
                  response = new SessionXAResponseMessage(true,
                                                          XAException.XAER_NOTA,
                                                          "Cannot find xid in resource manager: " + xid);
               }
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
                  theTx.commit(packet.isOnePhase());

                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
               }
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa commit", e);

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleXAEnd(final SessionXAEndMessage packet)
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

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleXAForget(final SessionXAForgetMessage packet)
   {
      long id = resourceManager.removeHeuristicCompletion(packet.getXid());
      int code = XAResource.XA_OK;
      if (id != -1)
      {
         try
         {
            storageManager.deleteHeuristicCompletion(id);
         }
         catch (Exception e)
         {
            e.printStackTrace();
            code = XAException.XAER_RMERR;
         }
      }
      else
      {
         code = XAException.XAER_NOTA;
      }

      Packet response = new SessionXAResponseMessage((code != XAResource.XA_OK), code, null);

      sendResponse(packet, response, false, false);
   }

   public void handleXAJoin(final SessionXAJoinMessage packet)
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

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleXAResume(final SessionXAResumeMessage packet)
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

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleXARollback(final SessionXARollbackMessage packet)
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
               // checked heuristic committed transactions
               if (resourceManager.getHeuristicCommittedTransactions().contains(xid))
               {
                  response = new SessionXAResponseMessage(true,
                                                          XAException.XA_HEURCOM,
                                                          "transaction has ben heuristically committed: " + xid);
               }
               // checked heuristic rolled back transactions
               else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid))
               {
                  response = new SessionXAResponseMessage(true,
                                                          XAException.XA_HEURRB,
                                                          "transaction has ben heuristically rolled back: " + xid);
               }
               else
               {
                  response = new SessionXAResponseMessage(true,
                                                          XAException.XAER_NOTA,
                                                          "Cannot find xid in resource manager: " + xid);
               }
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
                  doRollback(false, theTx);

                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
               }
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa rollback", e);

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleXAStart(final SessionXAStartMessage packet)
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

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleXASuspend(final Packet packet)
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

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleXAPrepare(final SessionXAPrepareMessage packet)
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
                  theTx.prepare();

                  response = new SessionXAResponseMessage(false, XAResource.XA_OK, null);
               }
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to xa prepare", e);

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleGetInDoubtXids(final Packet packet)
   {
      List<Xid> indoubtsXids = new ArrayList<Xid>();
      indoubtsXids.addAll(resourceManager.getPreparedTransactions());
      indoubtsXids.addAll(resourceManager.getHeuristicCommittedTransactions());
      indoubtsXids.addAll(resourceManager.getHeuristicRolledbackTransactions());
      Packet response = new SessionXAGetInDoubtXidsResponseMessage(indoubtsXids);

      sendResponse(packet, response, false, false);
   }

   public void handleGetXATimeout(final Packet packet)
   {
      Packet response = new SessionXAGetTimeoutResponseMessage(resourceManager.getTimeoutSeconds());

      sendResponse(packet, response, false, false);
   }

   public void handleSetXATimeout(final SessionXASetTimeoutMessage packet)
   {
      Packet response = new SessionXASetTimeoutResponseMessage(resourceManager.setTimeoutSeconds(packet.getTimeoutSeconds()));

      sendResponse(packet, response, false, false);
   }

   public void handleStart(final Packet packet)
   {
      setStarted(true);

      sendResponse(packet, null, false, false);
   }

   public void handleStop(final Packet packet)
   {
      final Packet response = new NullResponseMessage();

      setStarted(false);

      sendResponse(packet, response, false, false);
   }

   public void handleClose(final Packet packet)
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

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, true, true);

   }

   public void handleCloseConsumer(final SessionConsumerCloseMessage packet)
   {
      final ServerConsumer consumer = consumers.get(packet.getConsumerID());

      Packet response;

      try
      {
         if (consumer != null)
         {
            consumer.close();
         }
         else
         {
            log.error("Cannot find consumer with id " + packet.getConsumerID());
         }

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to close consumer", e);

         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleReceiveConsumerCredits(final SessionConsumerFlowCreditMessage packet)
   {
      ServerConsumer consumer = consumers.get(packet.getConsumerID());

      if (consumer == null)
      {
         log.error("There is no consumer with id " + packet.getConsumerID());
         return;
      }

      try
      {
         consumer.receiveCredits(packet.getCredits());
      }
      catch (Exception e)
      {
         log.error("Failed to receive credits " + server.getConfiguration().isBackup(), e);
      }

      sendResponse(packet, null, false, false);
   }

   public void handleSendLargeMessage(final SessionSendLargeMessage packet)
   {
      // need to create the LargeMessage before continue
      long id = storageManager.generateUniqueID();

      LargeServerMessage msg = doCreateLargeMessage(id, packet);

      if (msg != null)
      {
         if (currentLargeMessage != null)
         {
            log.warn("Replacing incomplete LargeMessage with ID=" + currentLargeMessage.getMessageID());
         }

         currentLargeMessage = msg;

         sendResponse(packet, null, false, false);
      }
   }

   public void handleSend(final SessionSendMessage packet)
   {
      Packet response = null;
      
      ServerMessage message = packet.getServerMessage();

      try
      {
         long id = storageManager.generateUniqueID();

         message.setMessageID(id);

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
            if (e instanceof HornetQException)
            {
               response = new HornetQExceptionMessage((HornetQException)e);
            }
            else
            {
               response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
            }
         }
      }
      finally
      {
         try
         {
            releaseOutStanding(message, message.getEncodeSize());
         }
         catch (Exception e)
         {
            log.error("Failed to release outstanding credits", e);
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleSendContinuations(final SessionSendContinuationMessage packet)
   {
      Packet response = null;

      try
      {
         if (currentLargeMessage == null)
         {
            throw new HornetQException(HornetQException.ILLEGAL_STATE, "large-message not initialized on server");
         }

         // Immediately release the credits for the continuations- these don't contrinute to the in-memory size
         // of the message

         releaseOutStanding(currentLargeMessage, packet.getRequiredBufferSize());

         currentLargeMessage.addBytes(packet.getBody());

         if (!packet.isContinues())
         {
            currentLargeMessage.releaseResources();

            send(currentLargeMessage);

            releaseOutStanding(currentLargeMessage, currentLargeMessage.getEncodeSize());

            currentLargeMessage = null;
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
            if (e instanceof HornetQException)
            {
               response = new HornetQExceptionMessage((HornetQException)e);
            }
            else
            {
               response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
            }
         }
      }

      sendResponse(packet, response, false, false);
   }

   public void handleRequestProducerCredits(final SessionRequestProducerCreditsMessage packet) throws Exception
   {
      final SimpleString address = packet.getAddress();

      final CreditManagerHolder holder = this.getCreditManagerHolder(address);

      int credits = packet.getCredits();

      int gotCredits = holder.manager.acquireCredits(credits, new CreditsAvailableRunnable()
      {
         public boolean run(final int credits)
         {
            synchronized (ServerSessionImpl.this)
            {
               if (!closed)
               {
                  sendProducerCredits(holder, credits, address);

                  return true;
               }
               else
               {
                  return false;
               }
            }
         }
      });

      if (gotCredits > 0)
      {
         sendProducerCredits(holder, gotCredits, address);
      }

      sendResponse(packet, null, false, false);
   }

   public int transferConnection(final RemotingConnection newConnection, final int lastReceivedCommandID)
   {
      boolean wasStarted = started;

      if (wasStarted)
      {
         setStarted(false);
      }

      remotingConnection.removeFailureListener(this);
      remotingConnection.removeCloseListener(this);

      // Note. We do not destroy the replicating connection here. In the case the live server has really crashed
      // then the connection will get cleaned up anyway when the server ping timeout kicks in.
      // In the case the live server is really still up, i.e. a split brain situation (or in tests), then closing
      // the replicating connection will cause the outstanding responses to be be replayed on the live server,
      // if these reach the client who then subsequently fails over, on reconnection to backup, it will have
      // received responses that the backup did not know about.

      channel.transferConnection(newConnection);

      newConnection.syncIDGeneratorSequence(remotingConnection.getIDGeneratorSequence());

      remotingConnection = newConnection;

      remotingConnection.addFailureListener(this);
      remotingConnection.addCloseListener(this);

      int serverLastReceivedCommandID = channel.getLastReceivedCommandID();

      channel.replayCommands(lastReceivedCommandID, id);

      if (wasStarted)
      {
         setStarted(true);
      }

      return serverLastReceivedCommandID;
   }

   public Channel getChannel()
   {
      return channel;
   }

   // FailureListener implementation
   // --------------------------------------------------------------------

   public void connectionFailed(final HornetQException me)
   {
      try
      {
         log.warn("Client connection failed, clearing up resources for session " + name);

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

         handleClose(new PacketImpl(PacketImpl.SESS_CLOSE));

         log.warn("Cleared up resources for session " + name);
      }
      catch (Throwable t)
      {
         log.error("Failed to close connection " + this);
      }
   }

   public void connectionClosed()
   {
      try
      {
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
      }
      catch (Throwable t)
      {
         log.error("Failed fire listeners " + this);
      }

   }

   // Public
   // ----------------------------------------------------------------------------

   // Private
   // ----------------------------------------------------------------------------

   /**
    * Respond to client after replication
    * @param packet
    * @param response
    */
   private void sendResponse(final Packet confirmPacket,
                             final Packet response,
                             final boolean flush,
                             final boolean closeChannel)
   {
      storageManager.afterCompleteOperations(new IOAsyncTask()
      {
         
         public void onError(int errorCode, String errorMessage)
         {
            log.warn("Error processing IOCallback code = " + errorCode + " message = " + errorMessage);

            HornetQExceptionMessage exceptionMessage = new HornetQExceptionMessage(new HornetQException(errorCode, errorMessage));
            
            doSendResponse(confirmPacket, exceptionMessage, flush, closeChannel);
         }
         
         public void done()
         {
            doSendResponse(confirmPacket, response, flush, closeChannel);
         }
      });
   }

   /**
    * @param confirmPacket
    * @param response
    * @param flush
    * @param closeChannel
    */
   private void doSendResponse(final Packet confirmPacket,
                               final Packet response,
                               final boolean flush,
                               final boolean closeChannel)
   {
      if (confirmPacket != null)
      {
         channel.confirm(confirmPacket);
         
         if (flush)
         {
            channel.flushConfirmations();
         }
      }

      if (response != null)
      {
         channel.send(response);
      }

      if (closeChannel)
      {
         channel.close();
      }
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

   /**
    * We need to create the LargeMessage before replicating the packet, or else we won't know how to extract the destination,
    * which is stored on the header
    * @param packet
    * @throws Exception
    */
   private LargeServerMessage doCreateLargeMessage(final long id, final SessionSendLargeMessage packet)
   {
      try
      {
         LargeServerMessage msg = createLargeMessageStorage(id, packet.getLargeMessageHeader());

         return msg;
      }
      catch (Exception e)
      {
         log.error("Failed to create large message", e);
         Packet response = null;

         channel.confirm(packet);
         if (response != null)
         {
            channel.send(response);
         }
         return null;
      }
   }

   private void handleManagementMessage(final ServerMessage message) throws Exception
   {
      try
      {
         securityStore.check(message.getDestination(), CheckType.MANAGE, this);
      }
      catch (HornetQException e)
      {
         if (!autoCommitSends)
         {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }

      ServerMessage reply = managementService.handleMessage(message);

      SimpleString replyTo = message.getSimpleStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME);

      if (replyTo != null)
      {
         reply.setDestination(replyTo);

         send(reply);
      }
   }

   private LargeServerMessage createLargeMessageStorage(final long id, final byte[] header) throws Exception
   {
      return storageManager.createLargeMessage(id, header);
   }

   private void doRollback(final boolean lastMessageAsDelived, final Transaction theTx) throws Exception
   {
      boolean wasStarted = started;

      List<MessageReference> toCancel = new ArrayList<MessageReference>();

      for (ServerConsumer consumer : consumers.values())
      {
         if (wasStarted)
         {
            consumer.setStarted(false);
         }

         toCancel.addAll(consumer.cancelRefs(lastMessageAsDelived, theTx));
      }

      for (MessageReference ref : toCancel)
      {
         ref.getQueue().cancel(theTx, ref);
      }

      theTx.rollback();

      if (wasStarted)
      {
         for (ServerConsumer consumer : consumers.values())
         {
            consumer.setStarted(true);
         }
      }
   }

   private void rollback(final boolean lastMessageAsDelived) throws Exception
   {
      if (tx == null)
      {
         // Might be null if XA

         tx = new TransactionImpl(storageManager);
      }

      doRollback(lastMessageAsDelived, tx);

      tx = new TransactionImpl(storageManager);
   }

   /*
    * The way flow producer flow control works is as follows:
    * The client can only send messages as long as it has credits. It requests credits from the server
    * which the server immediately sends if it has them. If the address is full it adds the request to
    * a list of waiting and the credits will get sent as soon as some free up.
    * The amount of real memory taken up by a message and references on the server is likely to be larger
    * than the encode size of the message. Therefore when a message arrives on the server, more credits
    * are taken corresponding to the real in memory size of the message and refs, and the original credits are
    * returned. When a session closes any outstanding credits will be returned.
    * 
    */
   private void releaseOutStanding(final ServerMessage message, final int credits) throws Exception
   {
      CreditManagerHolder holder = getCreditManagerHolder(message);

      holder.outstandingCredits -= credits;

      holder.store.returnProducerCredits(credits);
   }

   private CreditManagerHolder getCreditManagerHolder(final ServerMessage message) throws Exception
   {
      SimpleString address = message.getDestination();

      CreditManagerHolder holder = creditManagerHolders.get(address);

      if (holder == null)
      {
         holder = new CreditManagerHolder(message.getPagingStore());

         creditManagerHolders.put(address, holder);
      }

      return holder;
   }

   private CreditManagerHolder getCreditManagerHolder(final SimpleString address) throws Exception
   {
      CreditManagerHolder holder = creditManagerHolders.get(address);

      if (holder == null)
      {
         PagingStore store = postOffice.getPagingManager().getPageStore(address);

         holder = new CreditManagerHolder(store);

         creditManagerHolders.put(address, holder);
      }

      return holder;
   }

   private void sendProducerCredits(final CreditManagerHolder holder, final int credits, final SimpleString address)
   {      
      holder.outstandingCredits += credits;

      Packet packet = new SessionProducerCreditsMessage(credits, address, offset);

      channel.send(packet);
   }
   
   private void send(final ServerMessage msg) throws Exception
   {
      // Look up the paging store

      // check the user has write access to this address.
      try
      {
         securityStore.check(msg.getDestination(), CheckType.SEND, this);
      }
      catch (HornetQException e)
      {
         if (!autoCommitSends)
         {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }

      if (tx == null || autoCommitSends)
      {
         postOffice.route(msg);
      }
      else
      {
         postOffice.route(msg, tx);
      }
   }

   private static final class CreditManagerHolder
   {
      CreditManagerHolder(final PagingStore store)
      {
         this.store = store;

         manager = store.getProducerCreditManager();
      }

      final PagingStore store;

      final ServerProducerCreditManager manager;

      volatile int outstandingCredits;
   }
}
