/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.core.protocol.proton;

import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.engine.EndpointError;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.impl.LinkImpl;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPInternalErrorException;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.remoting.ReadyListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         4/10/13
 */
public class ProtonSession implements SessionCallback
{
   private final String name;

   private final ProtonRemotingConnection connection;

   private final HornetQServer server;

   private final TransportImpl protonTransport;

   private final ProtonProtocolManager protonProtocolManager;

   private ServerSession serverSession;

   private OperationContext context;

   //todo make this configurable
   private int tagCacheSize = 1000;

   private long currentTag = 0;

   private final List<byte[]> tagCache = new ArrayList<byte[]>();

   private Map<Object, ProtonProducer> producers = new HashMap<Object, ProtonProducer>();

   private Map<Long, ProtonConsumer> consumers = new HashMap<Long, ProtonConsumer>();

   public ProtonSession(String name, ProtonRemotingConnection connection, ProtonProtocolManager protonProtocolManager, OperationContext operationContext, HornetQServer server, TransportImpl protonTransport)
   {
      this.name = name;
      this.connection = connection;
      context = operationContext;
      this.server = server;
      this.protonTransport = protonTransport;
      this.protonProtocolManager = protonProtocolManager;
   }

   public ServerSession getServerSession()
   {
      return serverSession;
   }

   /*
   * we need to initialise the actual server session when we receive the first linkas this tells us whether or not the
   * session is transactional
   * */
   public void initialise(boolean transacted) throws HornetQAMQPInternalErrorException
   {
      if (serverSession == null)
      {
         try
         {
            serverSession = server.createSession(name,
                  connection.getLogin(),
                  connection.getPasscode(),
                  HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                  connection,
                  !transacted,
                  !transacted,
                  false,
                  false,
                  null,
                  this);
         }
         catch (Exception e)
         {
            throw HornetQMessageBundle.BUNDLE.errorCreatingHornetQSession(e.getMessage());
         }
      }
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address)
   {
   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address)
   {
   }

   @Override
   public int sendMessage(ServerMessage message, long consumerID, int deliveryCount)
   {
      ProtonConsumer protonConsumer = consumers.get(consumerID);
      if (protonConsumer != null)
      {
         return protonConsumer.handleDelivery(message, deliveryCount);
      }
      return 0;
   }

   @Override
   public int sendLargeMessage(ServerMessage message, long consumerID, long bodySize, int deliveryCount)
   {
      return 0;
   }

   @Override
   public int sendLargeMessageContinuation(long consumerID, byte[] body, boolean continues, boolean requiresResponse)
   {
      return 0;
   }

   @Override
   public void closed()
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void addReadyListener(ReadyListener listener)
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void removeReadyListener(ReadyListener listener)
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   public OperationContext getContext()
   {
      return context;
   }

   public void addProducer(Receiver receiver) throws HornetQAMQPException
   {
      try
      {
         ProtonProducer producer = new ProtonProducer(connection, this, protonProtocolManager, receiver);
         producer.init();
         producers.put(receiver, producer);
         receiver.setContext(producer);
         receiver.open();
      }
      catch (HornetQAMQPException e)
      {
         producers.remove(receiver);
         receiver.setTarget(null);
         ((LinkImpl) receiver).setLocalError(new EndpointError(e.getAmqpError(), e.getMessage()));
         receiver.close();
      }
   }


   public void addTransactionHandler(Coordinator coordinator, Receiver receiver)
   {
      TransactionHandler transactionHandler = new TransactionHandler(connection, coordinator, protonProtocolManager, this);
      receiver.setContext(transactionHandler);
      receiver.open();
      receiver.flow(100);
   }

   public void addConsumer(Sender sender) throws HornetQAMQPException
   {
      ProtonConsumer protonConsumer = new ProtonConsumer(connection, sender, this, server, protonProtocolManager);

      try
      {
         protonConsumer.init();
         consumers.put(protonConsumer.getConsumerID(), protonConsumer);
         sender.setContext(protonConsumer);
         sender.open();
         protonConsumer.start();
      }
      catch (HornetQAMQPException e)
      {
         consumers.remove(protonConsumer.getConsumerID());
         sender.setSource(null);
         ((LinkImpl) sender).setLocalError(new EndpointError(e.getAmqpError(), e.getMessage()));
         sender.close();
      }
   }

   public byte[] getTag()
   {
      synchronized (tagCache)
      {
         byte[] bytes;
         if(tagCache.size() > 0)
         {
            bytes = tagCache.remove(0);
         }
         else
         {
            bytes = Long.toHexString(currentTag++).getBytes();
         }
         return bytes;
      }
   }

   public void replaceTag(byte[] tag)
   {
      synchronized (tagCache)
      {
         if(tagCache.size() < tagCacheSize)
         {
            tagCache.add(tag);
         }
      }
   }

   public void close()
   {
      for (ProtonProducer protonProducer : producers.values())
      {
         try
         {
            protonProducer.close();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorClosingSession(e);
         }
      }
      producers.clear();
      for (ProtonConsumer protonConsumer : consumers.values())
      {
         try
         {
            protonConsumer.close();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorClosingConsumer(e);
         }
      }
      consumers.clear();
      try
      {
         getServerSession().rollback(true);
         getServerSession().close(false);
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.errorClosingSession(e);
      }
   }

   public void removeConsumer(long consumerID) throws HornetQAMQPException
   {
      consumers.remove(consumerID);
      try
      {
         getServerSession().closeConsumer(consumerID);
      }
      catch (Exception e)
      {
         throw HornetQMessageBundle.BUNDLE.errorClosingConsumer(consumerID, e.getMessage());
      }
   }

   public void removeProducer(Receiver receiver)
   {
      producers.remove(receiver);
   }
}
