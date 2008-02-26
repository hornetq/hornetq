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
package org.jboss.test.messaging.jms.network;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.KEEP_ALIVE_INTERVAL;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.KEEP_ALIVE_TIMEOUT;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;

import org.jboss.messaging.core.client.FailureListener;
import org.jboss.messaging.core.remoting.impl.RemotingConfiguration;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.server.MessagingException;
import org.jboss.test.messaging.jms.JMSTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ClientNetworkFailureTest extends JMSTestCase
{

   // Constants -----------------------------------------------------

   private MinaService minaService;
   private RemotingConfiguration originalRemotingConf;
   private NetworkFailureFilter networkFailureFilter;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ClientNetworkFailureTest(String name)
   {
      super(name);
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      minaService = (MinaService) servers.get(0).getMessagingServer()
            .getRemotingService();
      originalRemotingConf = minaService.getRemotingConfiguration();
      minaService.stop();
      RemotingConfiguration oldRemotingConfig = minaService
            .getRemotingConfiguration();
      RemotingConfiguration newRemotingConfig = new RemotingConfiguration(
            oldRemotingConfig);
      newRemotingConfig.setInvmDisabled(true);
      newRemotingConfig.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
      newRemotingConfig.setKeepAliveTimeout(KEEP_ALIVE_TIMEOUT);
      minaService.setRemotingConfiguration(newRemotingConfig);
      minaService.start();

      networkFailureFilter = new NetworkFailureFilter();
      minaService.getFilterChain().addFirst("network-failure",
            networkFailureFilter);

      assertActiveConnectionsOnTheServer(0);
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertActiveConnectionsOnTheServer(0);

      minaService.getFilterChain().remove("network-failure");

      minaService.stop();
      minaService.setRemotingConfiguration(originalRemotingConf);
      minaService.start();

      super.tearDown();
   }

   // Public --------------------------------------------------------

   public void testServerResourcesCleanUpWhenClientCommThrowsException()
         throws Exception
   {
      QueueConnection conn = getConnectionFactory().createQueueConnection();

      assertActiveConnectionsOnTheServer(1);

      final CountDownLatch exceptionLatch = new CountDownLatch(2);

      conn.setExceptionListener(new ExceptionListener()
      {

         public void onException(JMSException e)
         {
            exceptionLatch.countDown();
         }
      });
      FailureListener listener = new FailureListenerWithLatch(exceptionLatch);
      minaService.addFailureListener(listener);

      networkFailureFilter.messageSentThrowsException = new IOException(
            "Client is unreachable");
      networkFailureFilter.messageReceivedDropsPacket = true;

      boolean gotExceptionsOnTheServerAndTheClient = exceptionLatch.await(
            KEEP_ALIVE_INTERVAL + KEEP_ALIVE_TIMEOUT + 2, SECONDS);
      assertTrue(gotExceptionsOnTheServerAndTheClient);
      assertActiveConnectionsOnTheServer(0);

      try
      {
         conn.close();
         fail("close should fail since client resources must have been cleaned up on the server side");
      } catch (Exception e)
      {
      }

      minaService.removeFailureListener(listener);
   }

   public void testServerResourcesCleanUpWhenClientCommDropsPacket()
         throws Exception
   {
      QueueConnection conn = getConnectionFactory().createQueueConnection();

      final CountDownLatch exceptionLatch = new CountDownLatch(2);
      conn.setExceptionListener(new ExceptionListener()
      {
         public void onException(JMSException e)
         {
            log.warn("got expected exception on the client");
            exceptionLatch.countDown();
         }
      });

      FailureListener listener = new FailureListenerWithLatch(exceptionLatch);
      minaService.addFailureListener(listener);

      assertActiveConnectionsOnTheServer(1);

      networkFailureFilter.messageSentDropsPacket = true;
      networkFailureFilter.messageReceivedDropsPacket = true;

      boolean gotExceptionsOnTheServerAndTheClient = exceptionLatch.await(
            KEEP_ALIVE_INTERVAL + KEEP_ALIVE_TIMEOUT + 3, SECONDS);
      assertTrue(gotExceptionsOnTheServerAndTheClient);
      assertActiveConnectionsOnTheServer(0);

      try
      {
         conn.close();
         fail("close should fail since client resources must have been cleaned up on the server side");
      } catch (Exception e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private final class FailureListenerWithLatch implements FailureListener
   {
      private final CountDownLatch exceptionLatch;

      private FailureListenerWithLatch(CountDownLatch exceptionLatch)
      {
         this.exceptionLatch = exceptionLatch;
      }

      public void onFailure(MessagingException me)
      {
         log.warn("got expected exception on the server");
         exceptionLatch.countDown();
      }
   }
}
