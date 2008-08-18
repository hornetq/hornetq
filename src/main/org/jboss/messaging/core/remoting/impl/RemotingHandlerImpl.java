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

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.*;
import static org.jboss.messaging.util.DataConstants.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
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
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.OrderedExecutorFactory;

/**
 *
 * A RemotingHandlerImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RemotingHandlerImpl implements RemotingHandler
{
   private static final Logger log = Logger.getLogger(RemotingHandlerImpl.class);

   private final PacketDispatcher dispatcher;

   private final ExecutorFactory executorFactory;

   private final ConcurrentMap<Long, Executor> executors = new ConcurrentHashMap<Long, Executor>();

   private final ConcurrentMap<Object, Long> lastPings = new ConcurrentHashMap<Object, Long>();

   public RemotingHandlerImpl(final PacketDispatcher dispatcher, final ExecutorService executorService)
   {
      if (dispatcher == null)
      {
         throw new IllegalArgumentException ("argument dispatcher can't be null");
      }

      this.dispatcher = dispatcher;

      if (executorService != null)
      {
         executorFactory = new OrderedExecutorFactory(executorService);
      }
      else
      {
         executorFactory = null;
      }
   }

   public Set<Object> scanForFailedConnections(final long expirePeriod)
   {
      long now = System.currentTimeMillis();

      Set<Object> failedIDs = new HashSet<Object>();

      for (Map.Entry<Object, Long> entry: lastPings.entrySet())
      {
         long lastPing = entry.getValue();

         if (now - lastPing > expirePeriod)
         {
            failedIDs.add(entry.getKey());
         }
      }

      return failedIDs;
   }

   public void bufferReceived(final Object connectionID, final MessagingBuffer buffer) throws Exception
   {
      final Packet packet = decode(connectionID, buffer);

      if (executorFactory != null)
      {
         long executorID = packet.getExecutorID();

         Executor executor = executors.get(executorID);
         if (executor == null)
         {
            executor = executorFactory.getExecutor();

            Executor oldExecutor = executors.putIfAbsent(executorID, executor);

            if (oldExecutor != null)
            {
               //Avoid race
               executor = oldExecutor;
            }
         }

         executor.execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  dispatcher.dispatch(connectionID, packet);
               }
               catch (Exception e)
               {
                  log.error("unexpected error", e);
               }
            }
         });
      }
      else
      {
         dispatcher.dispatch(connectionID, packet);
      }
   }

   public int isReadyToHandle(final MessagingBuffer buffer)
   {
      if (buffer.remaining() <= SIZE_INT)
      {
         return -1;
      }

      int length = buffer.getInt();

      if (buffer.remaining() < length)
      {
         return -1;
      }

      return length;
   }

   public void closeExecutor(final long executorID)
   {
      if (executors != null)
      {
         executors.remove(executorID);
      }
   }

   public void removeLastPing(final Object connectionID)
   {
      lastPings.remove(connectionID);
   }

   // Public ------------------------------------------------------------------------------

   public int getNumExecutors()
   {
      return executors.size();
   }

   public Packet decode(final Object connectionID, final MessagingBuffer in) throws Exception
   {
      byte packetType = in.getByte();

      Packet packet;

      switch (packetType)
      {
         case PacketImpl.NULL:
         {
            packet = new PacketImpl(PacketImpl.NULL);
            break;
         }
         case PING:
         {
            lastPings.put(connectionID, System.currentTimeMillis());
            packet = new PacketImpl(PacketImpl.PING);
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
         case CLOSE:
         {
            packet = new PacketImpl(PacketImpl.CLOSE);
            break;
         }
         case CREATECONNECTION:
         {
            packet = new CreateConnectionRequest();
            break;
         }
         case CREATECONNECTION_RESP:
         {
            packet = new CreateConnectionResponse();
            break;
         }
         case PacketImpl.CONN_CREATESESSION:
         {
            packet = new ConnectionCreateSessionMessage();
            break;
         }
         case PacketImpl.CONN_CREATESESSION_RESP:
         {
            packet = new ConnectionCreateSessionResponseMessage();
            break;
         }
         case PacketImpl.CONN_START:
         {
            packet = new PacketImpl(PacketImpl.CONN_START);
            break;
         }
         case PacketImpl.CONN_STOP:
         {
            packet = new PacketImpl(PacketImpl.CONN_STOP);
            break;
         }
         case PacketImpl.SESS_CREATECONSUMER:
         {
            packet = new SessionCreateConsumerMessage();
            break;
         }
         case PacketImpl.SESS_CREATECONSUMER_RESP:
         {
            packet = new SessionCreateConsumerResponseMessage();
            break;
         }
         case PacketImpl.SESS_CREATEPRODUCER:
         {
            packet = new SessionCreateProducerMessage();
            break;
         }
         case PacketImpl.SESS_CREATEPRODUCER_RESP:
         {
            packet = new SessionCreateProducerResponseMessage();
            break;
         }
         case PacketImpl.SESS_CREATEBROWSER:
         {
            packet = new SessionCreateBrowserMessage();
            break;
         }
         case PacketImpl.SESS_CREATEBROWSER_RESP:
         {
            packet = new SessionCreateBrowserResponseMessage();
            break;
         }
         case PacketImpl.SESS_ACKNOWLEDGE:
         {
            packet = new SessionAcknowledgeMessage();
            break;
         }
         case PacketImpl.SESS_RECOVER:
         {
            packet = new PacketImpl(PacketImpl.SESS_RECOVER);
            break;
         }
         case PacketImpl.SESS_COMMIT:
         {
            packet = new PacketImpl(PacketImpl.SESS_COMMIT);
            break;
         }
         case PacketImpl.SESS_ROLLBACK:
         {
            packet = new PacketImpl(PacketImpl.SESS_ROLLBACK);
            break;
         }
         case PacketImpl.SESS_CANCEL:
         {
            packet = new SessionCancelMessage();
            break;
         }
         case PacketImpl.SESS_QUEUEQUERY:
         {
            packet = new SessionQueueQueryMessage();
            break;
         }
         case PacketImpl.SESS_QUEUEQUERY_RESP:
         {
            packet = new SessionQueueQueryResponseMessage();
            break;
         }
         case PacketImpl.SESS_CREATEQUEUE:
         {
            packet = new SessionCreateQueueMessage();
            break;
         }
         case PacketImpl.SESS_DELETE_QUEUE:
         {
            packet = new SessionDeleteQueueMessage();
            break;
         }
         case PacketImpl.SESS_ADD_DESTINATION:
         {
            packet = new SessionAddDestinationMessage();
            break;
         }
         case PacketImpl.SESS_REMOVE_DESTINATION:
         {
            packet = new SessionRemoveDestinationMessage();
            break;
         }
         case PacketImpl.SESS_BINDINGQUERY:
         {
            packet = new SessionBindingQueryMessage();
            break;
         }
         case PacketImpl.SESS_BINDINGQUERY_RESP:
         {
            packet = new SessionBindingQueryResponseMessage();
            break;
         }
         case PacketImpl.SESS_BROWSER_RESET:
         {
            packet = new PacketImpl(PacketImpl.SESS_BROWSER_RESET);
            break;
         }
         case PacketImpl.SESS_BROWSER_HASNEXTMESSAGE:
         {
            packet = new PacketImpl(PacketImpl.SESS_BROWSER_HASNEXTMESSAGE);
            break;
         }
         case PacketImpl.SESS_BROWSER_HASNEXTMESSAGE_RESP:
         {
            packet = new SessionBrowserHasNextMessageResponseMessage();
            break;
         }
         case PacketImpl.SESS_BROWSER_NEXTMESSAGE:
         {
            packet = new PacketImpl(PacketImpl.SESS_BROWSER_NEXTMESSAGE);
            break;
         }
         case PacketImpl.SESS_XA_START:
         {
            packet = new SessionXAStartMessage();
            break;
         }
         case PacketImpl.SESS_XA_END:
         {
            packet = new SessionXAEndMessage();
            break;
         }
         case PacketImpl.SESS_XA_COMMIT:
         {
            packet = new SessionXACommitMessage();
            break;
         }
         case PacketImpl.SESS_XA_PREPARE:
         {
            packet = new SessionXAPrepareMessage();
            break;
         }
         case PacketImpl.SESS_XA_RESP:
         {
            packet = new SessionXAResponseMessage();
            break;
         }
         case PacketImpl.SESS_XA_ROLLBACK:
         {
            packet = new SessionXARollbackMessage();
            break;
         }
         case PacketImpl.SESS_XA_JOIN:
         {
            packet = new SessionXAJoinMessage();
            break;
         }
         case PacketImpl.SESS_XA_SUSPEND:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
            break;
         }
         case PacketImpl.SESS_XA_RESUME:
         {
            packet = new SessionXAResumeMessage();
            break;
         }
         case PacketImpl.SESS_XA_FORGET:
         {
            packet = new SessionXAForgetMessage();
            break;
         }
         case PacketImpl.SESS_XA_INDOUBT_XIDS:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
            break;
         }
         case PacketImpl.SESS_XA_INDOUBT_XIDS_RESP:
         {
            packet = new SessionXAGetInDoubtXidsResponseMessage();
            break;
         }
         case PacketImpl.SESS_XA_SET_TIMEOUT:
         {
            packet = new SessionXASetTimeoutMessage();
            break;
         }
         case PacketImpl.SESS_XA_SET_TIMEOUT_RESP:
         {
            packet = new SessionXASetTimeoutResponseMessage();
            break;
         }
         case PacketImpl.SESS_XA_GET_TIMEOUT:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
            break;
         }
         case PacketImpl.SESS_XA_GET_TIMEOUT_RESP:
         {
            packet = new SessionXAGetTimeoutResponseMessage();
            break;
         }
         case PacketImpl.CONS_FLOWTOKEN:
         {
            packet = new ConsumerFlowCreditMessage();
            break;
         }
         case PacketImpl.PROD_SEND:
         {
            packet = new ProducerSendMessage();
            break;
         }
         case PacketImpl.PROD_RECEIVETOKENS:
         {
            packet = new ProducerFlowCreditMessage();
            break;
         }
         case PacketImpl.RECEIVE_MSG:
         {
            packet = new ReceiveMessage();
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

   // Private -----------------------------------------------------------------------------


}
