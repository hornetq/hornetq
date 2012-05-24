/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.server.cluster.impl;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession.BindingQuery;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.HornetQLogger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.FutureLatch;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;

/**
 * A Core BridgeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author Clebert Suconic
 *
 * Created 12 Nov 2008 11:37:35
 *
 *
 */

public class BridgeImpl implements Bridge, SessionFailureListener, SendAcknowledgementHandler
{
   // Constants -----------------------------------------------------

   private static final boolean isTrace = HornetQLogger.LOGGER.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private static final SimpleString JMS_QUEUE_ADDRESS_PREFIX = new SimpleString("jms.queue.");

   private static final SimpleString JMS_TOPIC_ADDRESS_PREFIX = new SimpleString("jms.topic.");

   protected final ServerLocatorInternal serverLocator;

   private final UUID nodeUUID;

   private final SimpleString name;

   private final Queue queue;

   protected final Executor executor;

   protected final ScheduledExecutorService scheduledExecutor;

   /** Used when there's a scheduled reconnection */
   protected ScheduledFuture<?> futureScheduledReconnection;

   private final Filter filter;

   private final SimpleString forwardingAddress;

   private final java.util.Queue<MessageReference> refs = new ConcurrentLinkedQueue<MessageReference>();

   private final Transformer transformer;

   private volatile ClientSessionFactoryInternal csf;

   protected volatile ClientSessionInternal session;

   private volatile ClientProducer producer;

   private volatile boolean started;

   private final boolean useDuplicateDetection;

   private volatile boolean active;
   
   private boolean deliveringLargeMessage;

   private final String user;

   private final String password;

   private boolean activated;

   private final int reconnectAttempts;

   private int reconnectAttemptsInUse;

   private final long retryInterval;

   private final double retryMultiplier;

   private final long maxRetryInterval;

   private int retryCount = 0;

   private NotificationService notificationService;

   private boolean stopping = false;

   public BridgeImpl(final ServerLocatorInternal serverLocator,
                     final int reconnectAttempts,
                     final long retryInterval,
                     final double retryMultiplier,
                     final long maxRetryInterval,
                     final UUID nodeUUID,
                     final SimpleString name,
                     final Queue queue,
                     final Executor executor,
                     final SimpleString filterString,
                     final SimpleString forwardingAddress,
                     final ScheduledExecutorService scheduledExecutor,
                     final Transformer transformer,
                     final boolean useDuplicateDetection,
                     final String user,
                     final String password,
                     final boolean activated,
                     final StorageManager storageManager) throws Exception
   {

      this.reconnectAttempts = reconnectAttempts;

      this.reconnectAttemptsInUse = -1;

      this.retryInterval = retryInterval;

      this.retryMultiplier = retryMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.serverLocator = serverLocator;

      this.nodeUUID = nodeUUID;

      this.name = name;

      this.queue = queue;

      this.executor = executor;

      this.scheduledExecutor = scheduledExecutor;

      filter = FilterImpl.createFilter(filterString);

      this.forwardingAddress = forwardingAddress;

      this.transformer = transformer;

      this.useDuplicateDetection = useDuplicateDetection;

      this.user = user;

      this.password = password;

      this.activated = activated;
   }

   public void setNotificationService(final NotificationService notificationService)
   {
      this.notificationService = notificationService;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      started = true;

      stopping = false;

      if (activated)
      {
         activate();
      }

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), NotificationType.BRIDGE_STARTED, props);
         notificationService.sendNotification(notification);
      }
   }

   public String debug()
   {
      return toString();
   }

   private void cancelRefs()
   {
      MessageReference ref;

      LinkedList<MessageReference> list = new LinkedList<MessageReference>();

      while ((ref = refs.poll()) != null)
      {
         if (isTrace)
         {
            HornetQLogger.LOGGER.trace("Cancelling reference " + ref + " on bridge " + this);
         }
         list.addFirst(ref);
      }

      if (isTrace && list.isEmpty())
      {
         HornetQLogger.LOGGER.trace("didn't have any references to cancel on bridge " + this);
      }

      Queue refqueue = null;

      long timeBase = System.currentTimeMillis();

      for (MessageReference ref2 : list)
      {
         refqueue = ref2.getQueue();

         try
         {
            refqueue.cancel(ref2, timeBase);
         }
         catch (Exception e)
         {
            // There isn't much we can do besides log an error
            HornetQLogger.LOGGER.errorCancellingRefOnBridge(e, ref2);
         }
      }
   }

   public void flushExecutor()
   {
      // Wait for any create objects runnable to complete
      FutureLatch future = new FutureLatch();

      executor.execute(future);

      boolean ok = future.await(10000);

      if (!ok)
      {
         HornetQLogger.LOGGER.timedOutWaitingToStopBridge();
      }
   }

   public void disconnect()
   {
      executor.execute(new Runnable()
      {
         public void run()
         {
            if (session != null)
            {
               try
               {
                  session.cleanUp(false);
               } catch (Exception dontcare)
               {
                  HornetQLogger.LOGGER.debug(dontcare.getMessage(), dontcare);
               }
               session = null;
            }
         }
      });
   }

   public boolean isConnected()
   {
      return session != null;
   }

   /** The cluster manager needs to use the same executor to close the serverLocator, otherwise the stop will break.
   *  This method is intended to expose this executor to the ClusterManager */
   public Executor getExecutor()
   {
      return executor;
   }

   public void stop() throws Exception
   {
      if (stopping)
      {
         return;
      }

      stopping = true;
      
      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug("Bridge " + this.name + " being stopped");
      }
      ClientSessionFactoryInternal localCSF = csf;

      if (localCSF != null)
         localCSF.cleanup();

      if (futureScheduledReconnection != null)
      {
         futureScheduledReconnection.cancel(true);
      }

      executor.execute(new StopRunnable());

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), NotificationType.BRIDGE_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            HornetQLogger.LOGGER.broadcastBridgeStoppedError(e);
         }
      }
   }

   public void pause() throws Exception
   {
      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug("Bridge " + this.name + " being paused");
      }

      executor.execute(new PauseRunnable());

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), NotificationType.BRIDGE_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            HornetQLogger.LOGGER.notificationBridgeStoppedError(e);
         }
      }
   }

   public void resume() throws Exception
   {
      queue.addConsumer(BridgeImpl.this);
      queue.deliverAsync();
   }

   public boolean isStarted()
   {
      return started;
   }

   public synchronized void activate()
   {
      activated = true;

      executor.execute(new ConnectRunnable());
   }

   public SimpleString getName()
   {
      return name;
   }

   public Queue getQueue()
   {
      return queue;
   }

   public Filter getFilter()
   {
      return filter;
   }

   public SimpleString getForwardingAddress()
   {
      return forwardingAddress;
   }

   public Transformer getTransformer()
   {
      return transformer;
   }

   public boolean isUseDuplicateDetection()
   {
      return useDuplicateDetection;
   }

   // For testing only
   public RemotingConnection getForwardingConnection()
   {
      if (session == null)
      {
         return null;
      }
      else
      {
         return session.getConnection();
      }
   }

   // SendAcknowledgementHandler implementation ---------------------

   public void sendAcknowledged(final Message message)
   {
      try
      {
         final MessageReference ref = refs.poll();

         if (ref != null)
         {
            if (isTrace)
            {
               HornetQLogger.LOGGER.trace(this + " Acking " + ref + " on queue " + ref.getQueue());
            }
            ref.getQueue().acknowledge(ref);
         }
      }
      catch (Exception e)
      {
         HornetQLogger.LOGGER.bridgeFailedToAck(e);
      }
   }

   // Consumer implementation ---------------------------------------

   /* Hook for processing message before forwarding */
   protected ServerMessage beforeForward(final ServerMessage message)
   {
      if (useDuplicateDetection)
      {
         // We keep our own DuplicateID for the Bridge, so bouncing back and forths will work fine
         byte[] bytes = getDuplicateBytes(nodeUUID, message.getMessageID());

         message.putBytesProperty(MessageImpl.HDR_BRIDGE_DUPLICATE_ID, bytes);
      }

      if (transformer != null)
      {
         final ServerMessage transformedMessage = transformer.transform(message);
         if (transformedMessage != message)
         {
            if (HornetQLogger.LOGGER.isDebugEnabled())
            {
               HornetQLogger.LOGGER.debug("The transformer " + transformer +
                         " made a copy of the message " +
                         message +
                         " as transformedMessage");
            }
         }
         return transformedMessage;
      }
      else
      {
         return message;
      }
   }

   /**
    * @param message
    * @return
    */
   public static byte[] getDuplicateBytes(final UUID nodeUUID, final long messageID)
   {
      byte[] bytes = new byte[24];

      ByteBuffer bb = ByteBuffer.wrap(bytes);

      bb.put(nodeUUID.asBytes());

      bb.putLong(messageID);

      return bytes;
   }

   public HandleStatus handle(final MessageReference ref) throws Exception
   {
      if (filter != null && !filter.match(ref.getMessage()))
      {
         return HandleStatus.NO_MATCH;
      }

      synchronized (this)
      {
         if (!active)
         {
            if (HornetQLogger.LOGGER.isDebugEnabled())
            {
               HornetQLogger.LOGGER.debug(this + "::Ignoring reference on bridge as it is set to iniactive ref=" + ref);
            }
            return HandleStatus.BUSY;
         }
         
         if (deliveringLargeMessage)
         {
            return HandleStatus.BUSY;
         }

         if (isTrace)
         {
            HornetQLogger.LOGGER.trace("Bridge " + this + " is handling reference=" + ref);
         }
         
         ref.handled();

         refs.add(ref);

         final ServerMessage message = beforeForward(ref.getMessage());

         final SimpleString dest;

         if (forwardingAddress != null)
         {
            dest = forwardingAddress;
         }
         else
         {
            // Preserve the original address
            dest = message.getAddress();
         }
         
         if (message.isLargeMessage())
         {
            deliveringLargeMessage = true;
            deliverLargeMessage(dest, ref, (LargeServerMessage)message);
            return HandleStatus.HANDLED;
         }
         else
         {
            return deliverStandardMessage(dest, ref, message);
         }
      }
   }

   // FailureListener implementation --------------------------------

   public void connectionFailed(final HornetQException me, boolean failedOver)
   {
      HornetQLogger.LOGGER.bridgeConnectionFailed(me, failedOver);

      try
      {
         if (producer != null)
         {
            producer.close();
         }

         csf.cleanup();
      }
      catch (Throwable dontCare)
      {
      }

      try
      {
         session.cleanUp(false);
      }
      catch (Throwable dontCare)
      {
      }

      fail(me.getType() == HornetQExceptionType.DISCONNECTED);

      tryScheduleRetryReconnect(me.getType());
   }

   protected void tryScheduleRetryReconnect(final HornetQExceptionType type)
   {
      scheduleRetryConnect();
   }

   public void beforeReconnect(final HornetQException exception)
   {
      // log.warn(name + "::Connection failed before reconnect ", exception);
      // fail(false);
   }

    private void deliverLargeMessage(final SimpleString dest,
                                    final MessageReference ref,
                                    final LargeServerMessage message)
   {
      executor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               producer.send(dest, message);
               
               // as soon as we are done sending the large message
               // we unset the delivery flag and we will call the deliveryAsync on the queue
               // so the bridge will be able to resume work
               unsetLargeMessageDelivery();
               
               if (queue != null)
               {
                  queue.deliverAsync();
               }
            }
            catch (final HornetQException e)
            {
               unsetLargeMessageDelivery();
               HornetQLogger.LOGGER.bridgeUnableToSendMessage(e, ref);

               executor.execute(new Runnable()
               {
                  public void run()
                  {
                     connectionFailed(e, false);
                  }
               });
            }
         }
      });
   }
   
   /**
    * @param ref
    * @param message
    * @return
    */
   private HandleStatus deliverStandardMessage(SimpleString dest, final MessageReference ref, ServerMessage message)
   {
      // if we failover during send then there is a chance that the
      // that this will throw a disconnect, we need to remove the message
      // from the acks so it will get resent, duplicate detection will cope
      // with any messages resent
  
      if (HornetQLogger.LOGGER.isTraceEnabled())
      {
         HornetQLogger.LOGGER.trace("going to send message " + message);
      }
  
      try
      {
         producer.send(dest, message);
      }
      catch (final HornetQException e)
      {
         HornetQLogger.LOGGER.bridgeUnableToSendMessage(e, ref);

         // We remove this reference as we are returning busy which means the reference will never leave the Queue.
         // because of this we have to remove the reference here
         refs.remove(ref);
         
         executor.execute(new Runnable()
         {
            public void run()
            {
               connectionFailed(e, false);
            }
         });
  
         return HandleStatus.BUSY;
      }
  
      return HandleStatus.HANDLED;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return this.getClass().getSimpleName() + "@" +
             Integer.toHexString(System.identityHashCode(this)) +
             " [name=" +
             name +
             ", queue=" +
             queue +
             " targetConnector=" +
             this.serverLocator +
             "]";
   }

   protected void fail(final boolean permanently)
   {
      HornetQLogger.LOGGER.debug(this + "\n\t::fail being called, permanently=" + permanently);

      if (queue != null)
      {
         try
         {
            if (isTrace)
            {
               HornetQLogger.LOGGER.trace("Removing consumer on fail " + this + " from queue " + queue);
            }
            queue.removeConsumer(this);
         }
         catch (Exception dontcare)
         {
            HornetQLogger.LOGGER.debug(dontcare);
         }
      }

      cancelRefs();
      if (queue != null)
      {
         queue.deliverAsync();
      }
   }

   /* Hook for doing extra stuff after connection */
   protected void afterConnect() throws Exception
   {
      retryCount = 0;
      reconnectAttemptsInUse = reconnectAttempts;
      if (futureScheduledReconnection != null)
      {
         futureScheduledReconnection.cancel(true);
         futureScheduledReconnection = null;
      }
   }

   /**
    * @return
    */
   protected ClientSessionFactoryInternal getCurrentFactory()
   {
      return csf;
   }

   /* Hook for creating session factory */
   protected ClientSessionFactoryInternal createSessionFactory() throws Exception
   {
      ClientSessionFactoryInternal csf = (ClientSessionFactoryInternal)serverLocator.createSessionFactory();
      csf.setReconnectAttempts(0);
      return csf;
   }

   /* This is called only when the bridge is activated */
   protected void connect()
   {
      HornetQLogger.LOGGER.debug("Connecting  " + this + " to its destination [" + nodeUUID.toString() + "], csf=" + this.csf);

      retryCount++;

      try
      {
         if (csf == null || csf.isClosed())
         {
            if (stopping)
               return;
            csf = createSessionFactory();
            if (csf == null)
            {
               // Retrying. This probably means the node is not available (for the cluster connection case)
               scheduleRetryConnect();
               return;
            }
            // Session is pre-acknowledge
            session = (ClientSessionInternal)csf.createSession(user, password, false, true, true, true, 1);
         }

         if (forwardingAddress != null)
         {
            BindingQuery query = null;

            try
            {
               query = session.bindingQuery(forwardingAddress);
            }
            catch (Throwable e)
            {
               HornetQLogger.LOGGER.errorQueryingBridge(e, name);
               // This was an issue during startup, we will not count this retry
               retryCount--;

               scheduleRetryConnectFixedTimeout(100);
               return;
            }

            if (forwardingAddress.startsWith(BridgeImpl.JMS_QUEUE_ADDRESS_PREFIX) || forwardingAddress.startsWith(BridgeImpl.JMS_TOPIC_ADDRESS_PREFIX))
            {
               if (!query.isExists())
               {
                  HornetQLogger.LOGGER.errorQueryingBridge(forwardingAddress, retryCount);
                  scheduleRetryConnect();
                  return;
               }
            }
            else
            {
               if (!query.isExists())
               {
                  HornetQLogger.LOGGER.bridgeNoBindings(getName(), getForwardingAddress(), getForwardingAddress());
               }
            }
         }

         producer = session.createProducer();
         session.addFailureListener(BridgeImpl.this);
         session.setSendAcknowledgementHandler(BridgeImpl.this);

         afterConnect();

         active = true;

         queue.addConsumer(BridgeImpl.this);
         queue.deliverAsync();

         HornetQLogger.LOGGER.bridgeConnected(this);

         return;
      }
      catch (HornetQException e)
      {
         // the session was created while its server was starting, retry it:
         if (e.getType() == HornetQExceptionType.SESSION_CREATION_REJECTED)
         {
            HornetQLogger.LOGGER.errorStartingBridge(name);

            // We are not going to count this one as a retry
            retryCount--;
            scheduleRetryConnectFixedTimeout(this.retryInterval);
            return;
         }
         else
         {
            if (HornetQLogger.LOGGER.isDebugEnabled())
            {
               HornetQLogger.LOGGER.debug("Bridge " + this + " is unable to connect to destination. Retrying", e);
            }

            scheduleRetryConnect();
         }
      }
      catch (Exception e)
      {
         HornetQLogger.LOGGER.errorConnectingBridge(e, this);
      }
   }

   protected void scheduleRetryConnect()
   {
      if (serverLocator.isClosed())
      {
         HornetQLogger.LOGGER.bridgeLocatorShutdown();
         return;
      }

      if (stopping)
      {
         HornetQLogger.LOGGER.bridgeStopping();
         return;
      }

      if (reconnectAttemptsInUse >= 0 && retryCount > reconnectAttempts)
      {
         HornetQLogger.LOGGER.bridgeAbortStart(name, retryCount, reconnectAttempts);
         fail(true);
         return;
      }

      long timeout = (long)(this.retryInterval * Math.pow(this.retryMultiplier, retryCount));
      if (timeout == 0)
      {
         timeout = this.retryInterval;
      }
      if (timeout > maxRetryInterval)
      {
         timeout = maxRetryInterval;
      }

      HornetQLogger.LOGGER.debug("Bridge " + this +
                " retrying connection #" +
                retryCount +
                ", maxRetry=" +
                reconnectAttemptsInUse +
                ", timeout=" +
                timeout);

      scheduleRetryConnectFixedTimeout(timeout);
   }

   protected void scheduleRetryConnectFixedTimeout(final long milliseconds)
   {
      if (csf != null)
      {
         try
         {
            csf.cleanup();
         }
         catch (Throwable ignored)
         {
         }
      }

      if (stopping)
         return;

      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug("Scheduling retry for bridge " + this.name + " in " + milliseconds + " milliseconds");
      }

      futureScheduledReconnection = scheduledExecutor.schedule(new FutureConnectRunnable(),
                                                               milliseconds,
                                                               TimeUnit.MILLISECONDS);
   }

   // Inner classes -------------------------------------------------

   private class StopRunnable implements Runnable
   {
      public void run()
      {
         try
         {
            HornetQLogger.LOGGER.debug("stopping bridge " + BridgeImpl.this);

            if (session != null)
            {
               HornetQLogger.LOGGER.debug("Cleaning up session " + session);
               session.removeFailureListener(BridgeImpl.this);
               try
               {
                  session.close();
                  session = null;
               }
               catch (Exception dontcare)
               {
               }
            }

            if (csf != null)
            {
               csf.cleanup();
            }

            queue.removeConsumer(BridgeImpl.this);

            internalCancelReferences();

            synchronized (BridgeImpl.this)
            {
               HornetQLogger.LOGGER.debug("Closing Session for bridge " + BridgeImpl.this.name);

               started = false;

               active = false;

            }

            if (isTrace)
            {
               HornetQLogger.LOGGER.trace("Removing consumer on stopRunnable " + this + " from queue " + queue);
            }
            HornetQLogger.LOGGER.bridgeStopped(name);
         }
         catch (Exception e)
         {
           HornetQLogger.LOGGER.error("Failed to stop bridge", e);
         }
      }
   }

   private class PauseRunnable implements Runnable
   {
      public void run()
      {
         try
         {
            synchronized (BridgeImpl.this)
            {
               started = false;

               active = false;
            }

            queue.removeConsumer(BridgeImpl.this);

            internalCancelReferences();

            HornetQLogger.LOGGER.bridgePaused(name);
         }
         catch (Exception e)
         {
            HornetQLogger.LOGGER.errorPausingBridge(e);
         }
      }

   }

   private void internalCancelReferences()
   {
      cancelRefs();

      if (queue != null)
      {
         queue.deliverAsync();
      }
   }

   /**
    * just set deliveringLargeMessage to false
    */
   private synchronized void unsetLargeMessageDelivery()
   {
      deliveringLargeMessage = false;
   }

   // The scheduling will still use the main executor here
   private class FutureConnectRunnable implements Runnable
   {
      public void run()
      {
         executor.execute(new ConnectRunnable());
      }
   }

   private final class ConnectRunnable implements Runnable
   {
      public synchronized void run()
      {
         if (!stopping)
         {
            connect();
         }
      }
   }
}
