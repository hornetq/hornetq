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
package org.jboss.jms.soak.example.reconnect;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.messaging.utils.TokenBucketLimiter;
import org.jboss.messaging.utils.TokenBucketLimiterImpl;

public class SoakReconnectableSender
{
   private static final Logger log = Logger.getLogger(SoakReconnectableSender.class.getName());

   public static void main(String[] args)
   {
      try
      {
         String fileName = SoakBase.getPerfFileName(args);

         SoakParams params = SoakBase.getParams(fileName);

         final SoakReconnectableSender sender = new SoakReconnectableSender(params);

         Runtime.getRuntime().addShutdownHook(new Thread()
         {
            @Override
            public void run()
            {
               sender.disconnect();
            }
         });

         sender.run();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private SoakParams perfParams;

   private Destination destination;

   private Connection connection;

   private Session session;

   private MessageProducer producer;

   private ExceptionListener exceptionListener = new ExceptionListener()
   {
      public void onException(JMSException e)
      {
         System.out.println("SoakReconnectableSender.exceptionListener.new ExceptionListener() {...}.onException()");
         disconnect();
         connect();
      }

   };

   private SoakReconnectableSender(final SoakParams perfParams)
   {
      this.perfParams = perfParams;
   }

   public void run() throws Exception
   {
      System.out.println("SoakReconnectableSender.run()");
      connect();

      BytesMessage message = session.createBytesMessage();

      byte[] payload = SoakBase.randomByteArray(perfParams.getMessageSize());

      message.writeBytes(payload);

      final int modulo = 10000;

      TokenBucketLimiter tbl = perfParams.getThrottleRate() != -1 ? new TokenBucketLimiterImpl(perfParams.getThrottleRate(),
                                                                                               false)
                                                                 : null;

      boolean transacted = perfParams.isSessionTransacted();
      int txBatchSize = perfParams.getBatchSize();
      boolean display = true;

      long start = System.currentTimeMillis();
      AtomicLong count = new AtomicLong(0);
      while (true)
      {
         try
         {
            while (true)
            {
               producer.send(message);
               count.incrementAndGet();

               if (transacted)
               {
                  if (count.longValue() % txBatchSize == 0)
                  {
                     session.commit();
                  }
               }

               if (display && (count.longValue() % modulo == 0))
               {
                  double duration = (1.0 * System.currentTimeMillis() - start) / 1000;
                  start = System.currentTimeMillis();
                  log.info(String.format("sent %s messages in %2.2fs", modulo, duration));
               }

               if (tbl != null)
               {
                  tbl.limit();
               }
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }

   private synchronized void disconnect()
   {
      if (connection != null)
      {
         try
         {
            connection.setExceptionListener(null);
            connection.close();
         }
         catch (JMSException e)
         {
            e.printStackTrace();
         }
         finally
         {
            connection = null;
         }
      }
   }

   private void connect()
   {
      InitialContext ic = null;
      try
      {
         ic = new InitialContext();

         ConnectionFactory factory = (ConnectionFactory)ic.lookup(perfParams.getConnectionFactoryLookup());

         destination = (Destination)ic.lookup(perfParams.getDestinationLookup());

         connection = factory.createConnection();

         session = connection.createSession(perfParams.isSessionTransacted(),
                                            perfParams.isDupsOK() ? Session.DUPS_OK_ACKNOWLEDGE
                                                                 : Session.AUTO_ACKNOWLEDGE);

         producer = session.createProducer(destination);

         producer.setDeliveryMode(perfParams.isDurable() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         producer.setDisableMessageID(perfParams.isDisableMessageID());

         producer.setDisableMessageTimestamp(perfParams.isDisableTimestamp());

         connection.setExceptionListener(exceptionListener);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         try
         {
            ic.close();
         }
         catch (NamingException e)
         {
            e.printStackTrace();
         }
      }
   }
}