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

import static org.hornetq.api.core.management.NotificationType.CONSUMER_CREATED;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.exception.HornetQXAException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.server.BindingQueryResult;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueQueryResult;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerConsumer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
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

   // Attributes ----------------------------------------------------------------------------

   // private final long id;

   private final String username;

   private final String password;

   private final int minLargeMessageSize;

   private final boolean autoCommitSends;

   private final boolean autoCommitAcks;

   private final boolean preAcknowledge;

   private final boolean strictUpdateDeliveryCount;

   private RemotingConnection remotingConnection;

   private final Map<Long, ServerConsumer> consumers = new ConcurrentHashMap<Long, ServerConsumer>();

   private Transaction tx;

   private final StorageManager storageManager;

   private final ResourceManager resourceManager;

   public final PostOffice postOffice;

   private final SecurityStore securityStore;

   private final ManagementService managementService;

   private volatile boolean started = false;

   private final Map<SimpleString, Runnable> failureRunners = new HashMap<SimpleString, Runnable>();

   private final String name;

   private final HornetQServer server;

   private final SimpleString managementAddress;

   // The current currentLargeMessage being processed
   private volatile LargeServerMessage currentLargeMessage;

   private boolean closed;

   private final Map<SimpleString, CreditManagerHolder> creditManagerHolders = new HashMap<SimpleString, CreditManagerHolder>();

   private final RoutingContext routingContext = new RoutingContextImpl(null);

   private final SessionCallback callback;

   // Constructors ---------------------------------------------------------------------------------

   public ServerSessionImpl(final String name,
                            final String username,
                            final String password,
                            final int minLargeMessageSize,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean preAcknowledge,
                            final boolean strictUpdateDeliveryCount,
                            final boolean xa,
                            final RemotingConnection remotingConnection,                     
                            final StorageManager storageManager,
                            final PostOffice postOffice,
                            final ResourceManager resourceManager,
                            final SecurityStore securityStore,
                            final ManagementService managementService,
                            final HornetQServer server,
                            final SimpleString managementAddress,
                            final SessionCallback callback) throws Exception
   {
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

      if (!xa)
      {
         tx = new TransactionImpl(storageManager);
      }

      this.strictUpdateDeliveryCount = strictUpdateDeliveryCount;

      this.managementService = managementService;

      this.name = name;

      this.server = server;

      this.managementAddress = managementAddress;

      this.callback = callback;
      
      remotingConnection.addFailureListener(this);

      remotingConnection.addCloseListener(this);
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

   private synchronized void doClose() throws Exception
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
            ServerSessionImpl.log.error("Failed to delete large message file", error);
         }
      }

      remotingConnection.removeFailureListener(this);

      // Return any outstanding credits

      closed = true;

      for (CreditManagerHolder holder : creditManagerHolders.values())
      {
         holder.store.returnProducerCredits(holder.outstandingCredits);
      }
      
      callback.closed();
   }

   public void createConsumer(final long consumerID,
                                    final SimpleString queueName,
                                    final SimpleString filterString,
                                    final boolean browseOnly) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE)
      {
         throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST, "Queue " + queueName + " does not exist");
      }

      securityStore.check(binding.getAddress(), CheckType.CONSUME, this);

      Filter filter = FilterImpl.createFilter(filterString);

      ServerConsumer consumer = new ServerConsumerImpl(consumerID,
                                                       this,
                                                       (QueueBinding)binding,
                                                       filter,
                                                       started,
                                                       browseOnly,
                                                       storageManager,
                                                       callback,
                                                       preAcknowledge,
                                                       strictUpdateDeliveryCount,
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
   }

   public void createQueue(final SimpleString address,
                                 final SimpleString name,
                                 final SimpleString filterString,
                                 final boolean temporary,
                                 final boolean durable) throws Exception
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

         failureRunners.put(name, new Runnable()
         {
            public void run()
            {
               try
               {
                  if (postOffice.getBinding(name) != null)
                  {
                     postOffice.removeBinding(name);

                     if (postOffice.getBindingsForAddress(name).getBindings().size() == 0)
                     {
                        creditManagerHolders.remove(name);
                     }
                  }
               }
               catch (Exception e)
               {
                  ServerSessionImpl.log.error("Failed to remove temporary queue " + name);
               }
            }
         });
      }
   }

   public void deleteQueue(final SimpleString name) throws Exception
   {
      Binding binding = postOffice.getBinding(name);

      if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE)
      {
         throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST);
      }

      server.destroyQueue(name, this);

      failureRunners.remove(name);

      if (postOffice.getBindingsForAddress(name).getBindings().size() == 0)
      {
         creditManagerHolders.remove(name);
      }
   }

   public QueueQueryResult executeQueueQuery(final SimpleString name) throws Exception
   {
      if (name == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }

      QueueQueryResult response;

      Binding binding = postOffice.getBinding(name);

      if (binding != null && binding.getType() == BindingType.LOCAL_QUEUE)
      {
         Queue queue = (Queue)binding.getBindable();

         Filter filter = queue.getFilter();

         SimpleString filterString = filter == null ? null : filter.getFilterString();

         response = new QueueQueryResult(name,
                                         binding.getAddress(),
                                         queue.isDurable(),
                                         queue.isTemporary(),
                                         filterString,
                                         queue.getConsumerCount(),
                                         queue.getMessageCount());
      }
      // make an exception for the management address (see HORNETQ-29)
      else if (name.equals(managementAddress))
      {
         response = new QueueQueryResult(name, managementAddress, true, false, null, -1, -1);
      }
      else
      {
         response = new QueueQueryResult();
      }

      return response;
   }

   public BindingQueryResult executeBindingQuery(final SimpleString address)
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

      return new BindingQueryResult(!names.isEmpty(), names);
   }

   public void forceConsumerDelivery(final long consumerID, final long sequence) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      consumer.forceDelivery(sequence);
   }

   public void acknowledge(final long consumerID, final long messageID) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      consumer.acknowledge(autoCommitAcks, tx, messageID);
   }

   public void expire(final long consumerID, final long messageID) throws Exception
   {
      MessageReference ref = consumers.get(consumerID).getExpired(messageID);

      if (ref != null)
      {
         ref.getQueue().expire(ref);
      }
   }

   public void commit() throws Exception
   {
      try
      {
         tx.commit();
      }
      finally
      {
         tx = new TransactionImpl(storageManager);
      }
   }

   public void rollback(final boolean considerLastMessageAsDelivered) throws Exception
   {
      if (tx == null)
      {
         // Might be null if XA

         tx = new TransactionImpl(storageManager);
      }

      doRollback(considerLastMessageAsDelivered, tx);

      tx = new TransactionImpl(storageManager);
   }

   public void xaCommit(final Xid xid, final boolean onePhase) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot commit, session is currently doing work in transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.removeTransaction(xid);

         if (theTx == null)
         {
            // checked heuristic committed transactions
            if (resourceManager.getHeuristicCommittedTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURCOM,
                                            "transaction has been heuristically committed: " + xid);
            }
            // checked heuristic rolled back transactions
            else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURRB,
                                            "transaction has been heuristically rolled back: " + xid);
            }
            else
            {
               throw new HornetQXAException(XAException.XAER_NOTA, "Cannot find xid in resource manager: " + xid);
            }
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               // Put it back
               resourceManager.putTransaction(xid, tx);

               throw new HornetQXAException(XAException.XAER_PROTO, "Cannot commit transaction, it is suspended " + xid);
            }
            else
            {
               theTx.commit(onePhase);
            }
         }
      }
   }

   public void xaEnd(final Xid xid) throws Exception
   {
      if (tx != null && tx.getXid().equals(xid))
      {
         if (tx.getState() == Transaction.State.SUSPENDED)
         {
            final String msg = "Cannot end, transaction is suspended";

            throw new HornetQXAException(XAException.XAER_PROTO, msg);
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

            throw new HornetQXAException(XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() != Transaction.State.SUSPENDED)
            {
               final String msg = "Transaction is not suspended " + xid;

               throw new HornetQXAException(XAException.XAER_PROTO, msg);
            }
            else
            {
               theTx.resume();
            }
         }
      }
   }

   public void xaForget(final Xid xid) throws Exception
   {
      long id = resourceManager.removeHeuristicCompletion(xid);

      if (id != -1)
      {
         try
         {
            storageManager.deleteHeuristicCompletion(id);
         }
         catch (Exception e)
         {
            e.printStackTrace();

            throw new HornetQXAException(XAException.XAER_RMERR);
         }
      }
      else
      {
         throw new HornetQXAException(XAException.XAER_NOTA);
      }
   }

   public void xaJoin(final Xid xid) throws Exception
   {
      Transaction theTx = resourceManager.getTransaction(xid);

      if (theTx == null)
      {
         final String msg = "Cannot find xid in resource manager: " + xid;

         throw new HornetQXAException(XAException.XAER_NOTA, msg);
      }
      else
      {
         if (theTx.getState() == Transaction.State.SUSPENDED)
         {
            throw new HornetQXAException(XAException.XAER_PROTO, "Cannot join tx, it is suspended " + xid);
         }
         else
         {
            tx = theTx;
         }
      }
   }

   public void xaResume(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot resume, session is currently doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;

            throw new HornetQXAException(XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() != Transaction.State.SUSPENDED)
            {
               throw new HornetQXAException(XAException.XAER_PROTO,
                                            "Cannot resume transaction, it is not suspended " + xid);
            }
            else
            {
               tx = theTx;

               tx.resume();
            }
         }
      }
   }

   public void xaRollback(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot roll back, session is currently doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.removeTransaction(xid);

         if (theTx == null)
         {
            // checked heuristic committed transactions
            if (resourceManager.getHeuristicCommittedTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURCOM,
                                            "transaction has ben heuristically committed: " + xid);
            }
            // checked heuristic rolled back transactions
            else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURRB,
                                            "transaction has ben heuristically rolled back: " + xid);
            }
            else
            {
               throw new HornetQXAException(XAException.XAER_NOTA, "Cannot find xid in resource manager: " + xid);
            }
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               // Put it back
               resourceManager.putTransaction(xid, tx);

               throw new HornetQXAException(XAException.XAER_PROTO,
                                            "Cannot rollback transaction, it is suspended " + xid);
            }
            else
            {
               doRollback(false, theTx);
            }
         }
      }
   }

   public void xaStart(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot start, session is already doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         tx = new TransactionImpl(xid, storageManager, postOffice);

         boolean added = resourceManager.putTransaction(xid, tx);

         if (!added)
         {
            final String msg = "Cannot start, there is already a xid " + tx.getXid();

            throw new HornetQXAException(XAException.XAER_DUPID, msg);
         }
      }
   }

   public void xaSuspend() throws Exception
   {
      if (tx == null)
      {
         final String msg = "Cannot suspend, session is not doing work in a transaction ";

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         if (tx.getState() == Transaction.State.SUSPENDED)
         {
            final String msg = "Cannot suspend, transaction is already suspended " + tx.getXid();

            throw new HornetQXAException(XAException.XAER_PROTO, msg);
         }
         else
         {
            tx.suspend();

            tx = null;
         }
      }
   }

   public void xaPrepare(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot commit, session is currently doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;

            throw new HornetQXAException(XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               throw new HornetQXAException(XAException.XAER_PROTO,
                                            "Cannot prepare transaction, it is suspended " + xid);
            }
            else
            {
               theTx.prepare();
            }
         }
      }
   }

   public List<Xid> xaGetInDoubtXids()
   {
      List<Xid> xids = new ArrayList<Xid>();

      xids.addAll(resourceManager.getPreparedTransactions());
      xids.addAll(resourceManager.getHeuristicCommittedTransactions());
      xids.addAll(resourceManager.getHeuristicRolledbackTransactions());

      return xids;
   }

   public int xaGetTimeout()
   {
      return resourceManager.getTimeoutSeconds();
   }

   public void xaSetTimeout(final int timeout)
   {
      resourceManager.setTimeoutSeconds(timeout);
   }

   public void start()
   {
      setStarted(true);
   }

   public void stop()
   {
      setStarted(false);
   }

   public void close()
   {
      storageManager.afterCompleteOperations(new IOAsyncTask()
      {
         public void onError(int errorCode, String errorMessage)
         {
         }

         public void done()
         {
            try
            {
               doClose();
            }
            catch (Exception e)
            {
               log.error("Failed to close session", e);
            }
         }
      });
   }

   public void closeConsumer(final long consumerID) throws Exception
   {
      final ServerConsumer consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         consumer.close();
      }
      else
      {
         ServerSessionImpl.log.error("Cannot find consumer with id " + consumerID);
      }
   }

   public void receiveConsumerCredits(final long consumerID, final int credits) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      if (consumer == null)
      {
         ServerSessionImpl.log.error("There is no consumer with id " + consumerID);

         return;
      }

      consumer.receiveCredits(credits);
   }

   public void sendLarge(final byte[] largeMessageHeader) throws Exception
   {
      // need to create the LargeMessage before continue
      long id = storageManager.generateUniqueID();

      LargeServerMessage msg = storageManager.createLargeMessage(id, largeMessageHeader);

      if (currentLargeMessage != null)
      {
         ServerSessionImpl.log.warn("Replacing incomplete LargeMessage with ID=" + currentLargeMessage.getMessageID());
      }

      currentLargeMessage = msg;
   }

   public void send(final ServerMessage message) throws Exception
   {
      try
      {
         long id = storageManager.generateUniqueID();

         message.setMessageID(id);
         message.encodeMessageIDToBuffer();

         if (message.getAddress().equals(managementAddress))
         {
            // It's a management message

            handleManagementMessage(message);
         }
         else
         {
            doSend(message);
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
            ServerSessionImpl.log.error("Failed to release outstanding credits", e);
         }
      }
   }

   public void sendContinuations(final int packetSize, final byte[] body, final boolean continues) throws Exception
   {
      if (currentLargeMessage == null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "large-message not initialized on server");
      }

      // Immediately release the credits for the continuations- these don't contribute to the in-memory size
      // of the message

      releaseOutStanding(currentLargeMessage, packetSize);

      currentLargeMessage.addBytes(body);

      if (!continues)
      {
         currentLargeMessage.releaseResources();

         doSend(currentLargeMessage);

         releaseOutStanding(currentLargeMessage, currentLargeMessage.getEncodeSize());

         currentLargeMessage = null;
      }
   }

   public void requestProducerCredits(final SimpleString address, final int credits) throws Exception
   {
      final CreditManagerHolder holder = getCreditManagerHolder(address);

      // Requesting -ve credits means returning them

      if (credits < 0)
      {
         releaseOutStanding(address, -credits);
      }
      else
      {
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
      }
   }

   public void setTransferring(final boolean transferring)
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.setTransferring(transferring);
      }
   }

   public void runConnectionFailureRunners()
   {
      for (Runnable runner : failureRunners.values())
      {
         try
         {
            runner.run();
         }
         catch (Throwable t)
         {
            ServerSessionImpl.log.error("Failed to execute failure runner", t);
         }
      }
   }

   // FailureListener implementation
   // --------------------------------------------------------------------

   public void connectionFailed(final HornetQException me)
   {
      try
      {
         ServerSessionImpl.log.warn("Client connection failed, clearing up resources for session " + name);

         for (Runnable runner : failureRunners.values())
         {
            try
            {
               runner.run();
            }
            catch (Throwable t)
            {
               ServerSessionImpl.log.error("Failed to execute failure runner", t);
            }
         }

         close();

         ServerSessionImpl.log.warn("Cleared up resources for session " + name);
      }
      catch (Throwable t)
      {
         ServerSessionImpl.log.error("Failed to close connection " + this);
      }
   }

   public void connectionClosed()
   {
      try
      {
         for (Runnable runner : failureRunners.values())
         {
            try
            {
               runner.run();
            }
            catch (Throwable t)
            {
               ServerSessionImpl.log.error("Failed to execute failure runner", t);
            }
         }
      }
      catch (Throwable t)
      {
         ServerSessionImpl.log.error("Failed to fire listeners " + this);
      }

   }

   // Public
   // ----------------------------------------------------------------------------

   // Private
   // ----------------------------------------------------------------------------

   private void setStarted(final boolean s)
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.setStarted(s);
      }

      started = s;
   }

   private void handleManagementMessage(final ServerMessage message) throws Exception
   {
      try
      {
         securityStore.check(message.getAddress(), CheckType.MANAGE, this);
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
         reply.setAddress(replyTo);

         doSend(reply);
      }
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
      releaseOutStanding(message.getAddress(), credits);
   }

   private void releaseOutStanding(final SimpleString address, final int credits) throws Exception
   {
      CreditManagerHolder holder = getCreditManagerHolder(address);

      holder.outstandingCredits -= credits;

      holder.store.returnProducerCredits(credits);
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

      callback.sendProducerCreditsMessage(credits, address, -1);
   }

   private void doSend(final ServerMessage msg) throws Exception
   {
      // Look up the paging store

      // check the user has write access to this address.
      try
      {
         securityStore.check(msg.getAddress(), CheckType.SEND, this);
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
      }
      else
      {
         routingContext.setTransaction(tx);
      }

      postOffice.route(msg, routingContext);

      routingContext.clear();
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
