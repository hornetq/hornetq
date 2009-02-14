/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.journal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.impl.journal.JournalStorageManager;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.integration.xa.BasicXaTest;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.SimpleString;

/**
 * A JournalCleanupIntegrationTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Feb 13, 2009 6:55:48 PM
 *
 *
 */
public class JournalCleanupIntegrationTest extends ServiceTestBase
{
   private static Logger log = Logger.getLogger(BasicXaTest.class);

   private final Map<String, AddressSettings> addressSettings = new HashMap<String, AddressSettings>();

   private MessagingService messagingService;

   private ClientSessionFactory sessionFactory;

   private Configuration configuration;

   private final SimpleString a1 = new SimpleString("a1");

   private final SimpleString a2 = new SimpleString("a2");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
      addressSettings.clear();
      configuration = createDefaultConfig();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setPagingDirectory(getPageDir());

      messagingService = createService(true, configuration, addressSettings);

      // start the server
      messagingService.start();

      sessionFactory = createInVMFactory();
      ClientSession clientSession;
      clientSession = sessionFactory.createSession(true, false, false);

      clientSession.createQueue(a1, a1, null, true, true);
      clientSession.createQueue(a2, a2, null, true, true);
      clientSession.close();
   }

   protected void tearDown() throws Exception
   {

      if (messagingService != null && messagingService.isStarted())
      {
         messagingService.stop();
      }

      super.tearDown();
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAutoCleanup() throws Exception
   {
      ClientSession clientSession = sessionFactory.createSession(false, true, true);

      ClientSession sessionConsumer = sessionFactory.createSession(null, null, false, true, true, false, 0);

      int NUMBER_OF_MESSAGES = 1000;

      CountDownLatch latch = new CountDownLatch(NUMBER_OF_MESSAGES);

      try
      {
         ClientProducer prod = clientSession.createProducer(a1);
         prod.send(createTextMessage(clientSession, "hello"));
         prod.close();

         ClientConsumer cons = sessionConsumer.createConsumer(a2);

         sessionConsumer.start();

         LocalHandler handler = new LocalHandler(latch);

         cons.setMessageHandler(handler);

         prod = clientSession.createProducer(a2);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            prod.send(createBytesMessage(clientSession, new byte[1024], true));
         }

         latch.await(10, TimeUnit.SECONDS);

         if (handler.e != null)
         {
            throw handler.e;
         }

         JournalStorageManager storage = (JournalStorageManager)messagingService.getServer().getStorageManager();
         JournalImpl journal = (JournalImpl)storage.getMessageJournal();

         // The cleanup is asynchronous, we keep trying the condition until a timeout of 15 seconds
         for (long timeout = System.currentTimeMillis() + 15000; timeout > System.currentTimeMillis();)
         {
            // Wait the current task to finish before we test the condition again
            journal.debugWait();

            if (journal.getDataFilesCount() <= 3)
            {
               break;
            }
         }

         assertTrue(journal.getDataFilesCount() <= 3);

         
         cons.close();
         cons = sessionConsumer.createConsumer(a1);
         
         ClientMessage mess = cons.receive(1000);
         
         assertNotNull(mess);
         mess.acknowledge();
         sessionConsumer.commit();
         
         journal.forceMoveNextFile();
         journal.checkAndReclaimFiles();

         assertEquals(0, journal.getDataFilesCount());
      }
      finally
      {
         clientSession.close();
         sessionConsumer.close();
      }

   }

   class LocalHandler implements MessageHandler
   {
      Exception e;

      CountDownLatch latch;

      LocalHandler(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void onMessage(ClientMessage message)
      {
         try
         {
            message.acknowledge();
            latch.countDown();
         }
         catch (MessagingException e)
         {
            this.e = e;
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
