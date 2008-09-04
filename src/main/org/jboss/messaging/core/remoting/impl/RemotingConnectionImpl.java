/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.remoting.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CREATESESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CREATESESSION_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.EXCEPTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.PING;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.PONG;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ADD_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BINDINGQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BINDINGQUERY_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_HASNEXTMESSAGE_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CANCEL;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CONSUMER_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATECONSUMER_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATEPRODUCER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATEPRODUCER_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATEQUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_DELETE_QUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_FLOWTOKEN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_NULL_RESPONSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_PACKETS_CONFIRMED;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_PRODUCER_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_QUEUEQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_QUEUEQUERY_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_RECEIVETOKENS;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_RECEIVE_MSG;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_RECOVER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_REMOVE_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_SEND;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_STOP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_END;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_FORGET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_GET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_GET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_INDOUBT_XIDS;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_INDOUBT_XIDS_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_JOIN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_PREPARE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_RESUME;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SUSPEND;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.BrowseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketsConfirmedMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserResetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionNullResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionProducerCloseMessage;
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
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.OrderedExecutorFactory;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt> $Id: RemotingConnectionImpl.java 4633
 *          2008-07-04 11:43:34Z timfox $
 */
public class RemotingConnectionImpl extends AbstractBufferHandler implements RemotingConnection
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingConnectionImpl.class);
   
   private static final float EXPIRE_FACTOR = 1.5f;

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private final Connection transportConnection;
   
   private final ConcurrentMap<Long, ChannelImpl> channels = new ConcurrentHashMap<Long, ChannelImpl>();

   private final List<FailureListener> failureListeners = new ArrayList<FailureListener>();

   private final long blockingCallTimeout;
   
   private final ExecutorFactory executorFactory;

   private final Runnable pinger;
   
   private final List<Interceptor> interceptors;
   
   private ScheduledFuture<?> future;

   private boolean firstTime = true;

   private volatile boolean gotPong;
  
   private volatile boolean destroyed;
   
   private long expirePeriod;
   
   private volatile boolean stopPinging;
   
   private volatile long expireTime = -1;
         
   private final Channel pingChannel;
      
   // Constructors
   // ---------------------------------------------------------------------------------

   /* Client side connection constructor */
   public RemotingConnectionImpl(final Connection transportConnection,               
                                 final long blockingCallTimeout, final long pingPeriod,
                                 final ExecutorService handlerExecutor,
                                 final ScheduledExecutorService pingExecutor,
                                 final List<Interceptor> interceptors)
                                 
   {
      this.transportConnection = transportConnection;

      this.blockingCallTimeout = blockingCallTimeout;
      
      if (handlerExecutor != null)
      {
         this.executorFactory = new OrderedExecutorFactory(handlerExecutor);
      }
      else
      {
         this.executorFactory = null;
      }

      this.interceptors = interceptors;
      
      //Channel zero is reserved for pinging
      pingChannel = getChannel(0, false, -1);
      
      pingChannel.setHandler(new PingPongHandler());
      
      if (pingPeriod != -1)
      {   
         pinger = new Pinger();
   
         expirePeriod = (long)(EXPIRE_FACTOR * pingPeriod);
   
         future = pingExecutor.scheduleWithFixedDelay(pinger, pingPeriod, pingPeriod,
                                                      TimeUnit.MILLISECONDS);
      }
      else
      {
         pinger = null;
      }
   }

   // RemotingConnection implementation
   // ------------------------------------------------------------

   public Object getID()
   {
      return transportConnection.getID();
   }
   
   public Channel getChannel(final long channelID, final boolean ordered,
                             final int packetConfirmationBatchSize)
   {
      ChannelImpl channel = new ChannelImpl(channelID, ordered, packetConfirmationBatchSize);
      
      ChannelImpl oldChannel = channels.putIfAbsent(channelID, channel);
      
      if (oldChannel != null)
      {
         channel = oldChannel;
      }
      
      return channel;
   }

   public synchronized void addFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      failureListeners.add(listener);
   }

   public synchronized boolean removeFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   public MessagingBuffer createBuffer(final int size)
   {
      return transportConnection.createBuffer(size);
   }

   public synchronized void fail(final MessagingException me)
   {
      log.warn(me.getMessage());

      destroy();

      // Then call the listeners
      for (FailureListener listener : new ArrayList<FailureListener>(failureListeners))
      {
         try
         {
            listener.connectionFailed(me);
         }
         catch (Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others from
            // executing
            log.error("Failed to execute failure listener", t);
         }
      }
   }

   public synchronized void destroy()
   {
      if (destroyed)
      {
         return;
      }

      if (future != null)
      {
         future.cancel(false);                  
      }
      
      pingChannel.close();
            
      channels.clear();

      destroyed = true;

      // We close the underlying transport connection
      transportConnection.close();
   }
   
   public boolean isExpired(final long now)
   {
      return expireTime != -1 && now >= expireTime;
   }
   
   /* For testing only */
   public void stopPingingAfterOne()
   {
      stopPinging = true;
   }

   // Buffer Handler implementation ----------------------------------------------------
   
   public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
   {
      final Packet packet = decode(buffer);
      
      long channelID = packet.getChannelID();
      
      ChannelImpl channel = channels.get(channelID);
      
      if (channel == null)
      {
         throw new IllegalArgumentException("Cannot handle packet " + packet + " no channel is registered with id " + channelID);
      }
      
      channel.handlePacket(packet);            
   }
        
   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void doWrite(final Packet packet)
   {
      if (destroyed)
      {
         throw new IllegalStateException("Cannot write packet to connection, it is destroyed");
      }

      MessagingBuffer buffer = transportConnection.createBuffer(PacketImpl.INITIAL_BUFFER_SIZE);

      packet.encode(buffer);

      transportConnection.write(buffer);
   }

   
   private Packet decode(final MessagingBuffer in)
   {
      byte packetType = in.getByte();

      Packet packet;

      switch (packetType)
      {
         case PING:
         {            
            packet = new Ping();
           // packet.decode(in);
          //  expireTimes.put(connectionID, System.currentTimeMillis() + ((Ping)packet).getExpirePeriod());
          //  return packet;
            break;
         }
         case PONG:
         {
            packet = new PacketImpl(PacketImpl.PONG);
            break;
         }
         case EXCEPTION:
         {
            packet = new MessagingExceptionMessage();
            break;
         }
         case CREATESESSION:
         {
            packet = new CreateSessionMessage();
            break;
         }
         case CREATESESSION_RESP:
         {
            packet = new CreateSessionResponseMessage();
            break;
         }
         case SESS_CREATECONSUMER:
         {
            packet = new SessionCreateConsumerMessage();
            break;
         }
         case SESS_CREATECONSUMER_RESP:
         {
            packet = new SessionCreateConsumerResponseMessage();
            break;
         }
         case SESS_CREATEPRODUCER:
         {
            packet = new SessionCreateProducerMessage();
            break;
         }
         case SESS_CREATEPRODUCER_RESP:
         {
            packet = new SessionCreateProducerResponseMessage();
            break;
         }
         case SESS_CREATEBROWSER:
         {
            packet = new SessionCreateBrowserMessage();
            break;
         }
         case SESS_ACKNOWLEDGE:
         {
            packet = new SessionAcknowledgeMessage();
            break;
         }
         case SESS_RECOVER:
         {
            packet = new PacketImpl(PacketImpl.SESS_RECOVER);
            break;
         }
         case SESS_COMMIT:
         {
            packet = new PacketImpl(PacketImpl.SESS_COMMIT);
            break;
         }
         case SESS_ROLLBACK:
         {
            packet = new PacketImpl(PacketImpl.SESS_ROLLBACK);
            break;
         }
         case SESS_CANCEL:
         {
            packet = new SessionCancelMessage();
            break;
         }
         case SESS_QUEUEQUERY:
         {
            packet = new SessionQueueQueryMessage();
            break;
         }
         case SESS_QUEUEQUERY_RESP:
         {
            packet = new SessionQueueQueryResponseMessage();
            break;
         }
         case SESS_CREATEQUEUE:
         {
            packet = new SessionCreateQueueMessage();
            break;
         }
         case SESS_DELETE_QUEUE:
         {
            packet = new SessionDeleteQueueMessage();
            break;
         }
         case SESS_ADD_DESTINATION:
         {
            packet = new SessionAddDestinationMessage();
            break;
         }
         case SESS_REMOVE_DESTINATION:
         {
            packet = new SessionRemoveDestinationMessage();
            break;
         }
         case SESS_BINDINGQUERY:
         {
            packet = new SessionBindingQueryMessage();
            break;
         }
         case SESS_BINDINGQUERY_RESP:
         {
            packet = new SessionBindingQueryResponseMessage();
            break;
         }
         case PacketImpl.SESS_BROWSER_MESSAGE:
         {
            packet = new BrowseMessage();
            break;
         }
         case SESS_BROWSER_RESET:
         {
            packet = new SessionBrowserResetMessage();
            break;
         }
         case SESS_BROWSER_HASNEXTMESSAGE:
         {
            packet = new SessionBrowserHasNextMessageMessage();
            break;
         }
         case SESS_BROWSER_HASNEXTMESSAGE_RESP:
         {
            packet = new SessionBrowserHasNextMessageResponseMessage();
            break;
         }
         case SESS_BROWSER_NEXTMESSAGE:
         {
            packet = new SessionBrowserNextMessageMessage();
            break;
         }
         case SESS_XA_START:
         {
            packet = new SessionXAStartMessage();
            break;
         }
         case SESS_XA_END:
         {
            packet = new SessionXAEndMessage();
            break;
         }
         case SESS_XA_COMMIT:
         {
            packet = new SessionXACommitMessage();
            break;
         }
         case SESS_XA_PREPARE:
         {
            packet = new SessionXAPrepareMessage();
            break;
         }
         case SESS_XA_RESP:
         {
            packet = new SessionXAResponseMessage();
            break;
         }
         case SESS_XA_ROLLBACK:
         {
            packet = new SessionXARollbackMessage();
            break;
         }
         case SESS_XA_JOIN:
         {
            packet = new SessionXAJoinMessage();
            break;
         }
         case SESS_XA_SUSPEND:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
            break;
         }
         case SESS_XA_RESUME:
         {
            packet = new SessionXAResumeMessage();
            break;
         }
         case SESS_XA_FORGET:
         {
            packet = new SessionXAForgetMessage();
            break;
         }
         case SESS_XA_INDOUBT_XIDS:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
            break;
         }
         case SESS_XA_INDOUBT_XIDS_RESP:
         {
            packet = new SessionXAGetInDoubtXidsResponseMessage();
            break;
         }
         case SESS_XA_SET_TIMEOUT:
         {
            packet = new SessionXASetTimeoutMessage();
            break;
         }
         case SESS_XA_SET_TIMEOUT_RESP:
         {
            packet = new SessionXASetTimeoutResponseMessage();
            break;
         }
         case SESS_XA_GET_TIMEOUT:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
            break;
         }
         case SESS_XA_GET_TIMEOUT_RESP:
         {
            packet = new SessionXAGetTimeoutResponseMessage();
            break;
         }
         case SESS_START:
         {            
            packet = new PacketImpl(PacketImpl.SESS_START);
            break;
         }
         case SESS_STOP:
         {
            packet = new PacketImpl(PacketImpl.SESS_STOP);
            break;
         }
         case SESS_FLOWTOKEN:
         {
            packet = new SessionFlowCreditMessage();
            break;
         }
         case SESS_SEND:
         {
            packet = new SendMessage();
            break;
         }
         case SESS_RECEIVETOKENS:
         {
            packet = new ProducerFlowCreditMessage();
            break;
         }
         case SESS_RECEIVE_MSG:
         {
            packet = new ReceiveMessage();
            break;
         }
         case SESS_PACKETS_CONFIRMED:
         {
            packet = new PacketsConfirmedMessage();
            break;
         }
         case SESS_CLOSE:
         {
            packet = new PacketImpl(PacketImpl.SESS_CLOSE);
            break;
         }
         case SESS_CONSUMER_CLOSE:
         {
            packet = new SessionConsumerCloseMessage();
            break;
         }
         case SESS_PRODUCER_CLOSE:
         {
            packet = new SessionProducerCloseMessage();
            break;
         }
         case SESS_BROWSER_CLOSE:
         {
            packet = new SessionBrowserCloseMessage();
            break;
         }
         case SESS_NULL_RESPONSE:
         {
            packet = new SessionNullResponseMessage();
            break;
         }
         default:
         {
            throw new IllegalArgumentException("Invalid type: " + packetType);
         }
      }

      packet.decode(in);

      return packet;
   }

   // Inner classes
   // --------------------------------------------------------------------------------
   
   private class ChannelImpl implements Channel
   {
      private final long id;
      
      private final Executor executor;
      
      private ChannelHandler handler;
      
      private Packet response;
      
      private final java.util.Queue<Packet> resendCache;
      
      private final int packetConfirmationBatchSize;
      
      private volatile int firstStoredCommandID;
      
      private volatile int lastReceivedCommandID = -1;
      
      private volatile int nextConfirmation;
         
      public ChannelImpl(final long id, final boolean ordered, final int packetConfirmationBatchSize)
      {
         this.id = id;
         
         if (ordered && executorFactory != null)
         {              
            executor = executorFactory.getExecutor();            
         }
         else
         {
            executor = null;
         }                  
         
         this.packetConfirmationBatchSize = packetConfirmationBatchSize;
         
         if (packetConfirmationBatchSize != -1)
         {
            resendCache = new ConcurrentLinkedQueue<Packet>();
            
            this.nextConfirmation = packetConfirmationBatchSize - 1;
         }
         else
         {
            resendCache = null;
         }
      }
         
      public void send(final Packet packet)
      {
         packet.setChannelID(id);
         
         if (packetConfirmationBatchSize != -1)
         {
            addToCache(packet);
         }
         
         doWrite(packet);
      }

      public synchronized Packet sendBlocking(final Packet packet) throws MessagingException
      {
         response = null;
                  
         packet.setChannelID(id);

         if (packetConfirmationBatchSize != -1)
         {
            addToCache(packet);
         }
         
         doWrite(packet);
         
         long toWait = blockingCallTimeout;
         
         long start = System.currentTimeMillis();

         while (response == null && toWait > 0)
         {
            try
            {
               wait(toWait);
            }
            catch (InterruptedException e)
            {
            }

            long now = System.currentTimeMillis();

            toWait -= now - start;

            start = now;
         }
         
         if (response == null)
         {
            throw new IllegalStateException("Timed out waiting for response");
         }
         
         if (response.getType() == PacketImpl.EXCEPTION)
         {
            MessagingExceptionMessage mem = (MessagingExceptionMessage)response;
            
            throw mem.getException();
         }
         else
         {
            return response;
         }
      }

      public void setHandler(final ChannelHandler handler)
      {
         this.handler = handler;
      }
      
      public void close()
      {
         if (!destroyed && channels.remove(id) == null)
         {
            throw new IllegalArgumentException("Cannot find channel with id " + id + " to close");
         }         
      }
      
      private void handlePacket(final Packet packet)
      {
         if (packet.getType() == PacketImpl.SESS_PACKETS_CONFIRMED)
         {
            PacketsConfirmedMessage msg = (PacketsConfirmedMessage)packet;
            
            clearUpTo(msg.getCommandID());
         }    
         else if (packet.isResponse())
         {
            synchronized (this)
            {
               response = packet;
               
               notify();
            }
         }         
         else if (handler != null)
         {
            if (executor == null)
            {
               doHandle(packet);
            }
            else
            {
               executor.execute(new Runnable()
               {
                  public void run()
                  {
                     doHandle(packet);
                  }
               });
            }            
         }
      }
      
      private void doHandle(final Packet packet)
      {
         if (interceptors != null)
         {
            for (Interceptor interceptor : interceptors)
            {
               try
               {
                  boolean callNext = interceptor.intercept(packet, RemotingConnectionImpl.this);
                  
                  if (!callNext)
                  {
                     //abort
                     
                     return;
                  }
               }
               catch (Throwable e)
               {
                  log.warn("Failure in calling interceptor: " + interceptor, e);
               }
            }
         }
                           
         handler.handlePacket(packet);
         
         if (packetConfirmationBatchSize != -1)
         {
            lastReceivedCommandID++;
            
            if (lastReceivedCommandID == nextConfirmation)
            {
               Packet confirmed = new PacketsConfirmedMessage(lastReceivedCommandID);
               
               nextConfirmation += packetConfirmationBatchSize;
               
               confirmed.setChannelID(id);
                
               doWrite(confirmed);
            }                        
         }
      }      
      
      private void addToCache(final Packet packet)
      {      
         resendCache.add(packet);
      }
      
      private void clearUpTo(final int lastReceivedCommandID)
      {
         int numberToClear = 1 + lastReceivedCommandID - firstStoredCommandID;
           
         if (numberToClear == -1)
         {
            throw new IllegalArgumentException("Invalid lastReceivedCommandID: " + lastReceivedCommandID);
         }
         
         for (int i = 0; i < numberToClear; i++)
         {
            Packet packet = resendCache.poll();
            
            if (packet == null)
            {
               throw new IllegalStateException("Can't find packet to clear");
            }
         }
         
         firstStoredCommandID += numberToClear;
      }
   }   
      
   private class Pinger implements Runnable
   {
      public synchronized void run()
      {
         if (!firstTime && !gotPong)
         {
            // Error - didn't get pong back
            MessagingException me = new MessagingException(MessagingException.NOT_CONNECTED,
                                                           "Did not receive pong from server");

            fail(me);
         }

         gotPong = false;
         
         firstTime = false;

         // Send ping
         Packet ping = new Ping(expirePeriod);
         
         pingChannel.send(ping);
      }
   }
   
   private class PingPongHandler implements ChannelHandler
   {
      public void handlePacket(final Packet packet)
      {
         byte type = packet.getType();
         
         if (type == PONG)
         {            
            gotPong = true;

            if (stopPinging)
            {
               future.cancel(true);
            }
         }
         else if (type == PING)
         {
            expireTime = System.currentTimeMillis() + ((Ping)packet).getExpirePeriod();
            
            //Parameter is placeholder for future
            Packet pong = new Pong(-1);
            
            pingChannel.send(pong);
         }
         else
         {
            throw new IllegalArgumentException("Invalid packet: " + packet);
         }
      }

   }

}
