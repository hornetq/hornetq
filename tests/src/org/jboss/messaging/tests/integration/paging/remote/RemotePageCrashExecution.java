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

package org.jboss.messaging.tests.integration.paging.remote;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PagedMessage;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.impl.PagingManagerImpl;
import org.jboss.messaging.core.paging.impl.PagingStoreFactoryNIO;
import org.jboss.messaging.core.paging.impl.PagingStoreImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.persistence.impl.journal.JournalStorageManager;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.impl.JBMSecurityManagerImpl;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.OrderedExecutorFactory;
import org.jboss.messaging.util.SimpleString;

/**
 * A RemotePageCrashExecution
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Jan 7, 2009 6:41:41 PM
 *
 *
 */
public class RemotePageCrashExecution extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   private static final Logger log = Logger.getLogger(RemotePageCrashExecution.class);

   public static void main(final String arg[])
   {
      try
      {
         RemotePageCrashExecution execution = new RemotePageCrashExecution();
         execution.pageAndFail();
      }
      catch (Exception e)
      {
         log.warn(e.getMessage(), e);
      }

   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void pageAndFail() throws Exception
   {
      clearData();
      Configuration config = createDefaultConfig();

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingDefaultSize(10 * 1024);

      MessagingService service = newMessagingService(config);

      service.start();

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true, false);

         ClientProducer producer = session.createProducer(ADDRESS);

         ByteBuffer ioBuffer = ByteBuffer.allocate(1024);

         ClientMessage message = null;

         MessagingBuffer bodyLocal = new ByteBufferWrapper(ioBuffer);

         message = session.createClientMessage(true);
         message.setBody(bodyLocal);

         PagingStore store = service.getServer().getPostOffice().getPagingManager().getPageStore(ADDRESS);

         int messages = 0;
         while (!store.isPaging())
         {
            producer.send(message);
            messages++;
         }

         for (int i = 0; i < 2; i++)
         {
            messages++;
            producer.send(message);
         }

         session.close();

         assertTrue(service.getServer().getPostOffice().getPagingManager().getGlobalSize() > 0);

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < messages; i++)
         {
            ClientMessage message2 = consumer.receive(10000);

            assertNotNull(message2);

            message2.acknowledge();
         }

         consumer.close();

         session.close();

         assertEquals(0, service.getServer().getPostOffice().getPagingManager().getGlobalSize());

      }
      finally
      {
         try
         {
            service.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private MessagingServiceImpl newMessagingService(final Configuration configuration)
   {

      StorageManager storageManager = new JournalStorageManager(configuration);

      RemotingService remotingService = new RemotingServiceImpl(configuration);

      JBMSecurityManager securityManager = new JBMSecurityManagerImpl(true);

      ManagementService managementService = new ManagementServiceImpl(ManagementFactory.getPlatformMBeanServer(), false);

      remotingService.setManagementService(managementService);

      MessagingServer server = new FailingMessagingServiceImpl();

      server.setConfiguration(configuration);

      server.setStorageManager(storageManager);

      server.setRemotingService(remotingService);

      server.setSecurityManager(securityManager);

      server.setManagementService(managementService);

      return new MessagingServiceImpl(server, storageManager, remotingService);
   }

   // Inner classes -------------------------------------------------

   /** This is hacking MessagingServerImpl, 
    *  to make sure the server will fail right 
    *  after before the page-file was removed */
   class FailingMessagingServiceImpl extends MessagingServerImpl
   {
      /**
       * Method could be replaced for test purposes 
       */
      @Override
      protected PagingManager createPagingManager()
      {
         return new PagingManagerImpl(new FailurePagingStoreFactoryNIO(super.getConfiguration().getPagingDirectory(),
                                                                       super.getConfiguration().getPagingMaxThreads()),
                                      super.getStorageManager(),
                                      super.getQueueSettingsRepository(),
                                      super.getConfiguration().getPagingMaxGlobalSizeBytes(),
                                      super.getConfiguration().getPagingDefaultSize(),
                                      super.getConfiguration().isJournalSyncNonTransactional());
      }

      class FailurePagingStoreFactoryNIO extends PagingStoreFactoryNIO

      {
         /**
          * @param directory
          * @param maxThreads
          */
         public FailurePagingStoreFactoryNIO(final String directory, final int maxThreads)
         {
            super(directory, maxThreads);
         }

         // Constants -----------------------------------------------------

         // Attributes ----------------------------------------------------

         // Static --------------------------------------------------------

         // Constructors --------------------------------------------------

         // Public --------------------------------------------------------

         @Override
         public synchronized PagingStore newStore(final SimpleString destinationName, final QueueSettings settings) throws Exception
         {
            Field factoryField = PagingStoreFactoryNIO.class.getDeclaredField("executorFactory");
            factoryField.setAccessible(true);

            OrderedExecutorFactory factory = (OrderedExecutorFactory)factoryField.get(this);
            return new FailingPagingStore(destinationName, settings, factory.getExecutor());
         }

         // Package protected ---------------------------------------------

         // Protected -----------------------------------------------------

         // Private -------------------------------------------------------

         // Inner classes -------------------------------------------------
         class FailingPagingStore extends PagingStoreImpl
         {

            /**
             * @param pagingManager
             * @param storageManager
             * @param postOffice
             * @param fileFactory
             * @param storeFactory
             * @param storeName
             * @param queueSettings
             * @param executor
             */
            public FailingPagingStore(final SimpleString storeName,
                                      final QueueSettings queueSettings,
                                      final Executor executor)
            {
               super(getPostOffice().getPagingManager(),
                     getStorageManager(),
                     getPostOffice(),
                     null,
                     FailurePagingStoreFactoryNIO.this,
                     storeName,
                     queueSettings,
                     executor);
            }

            @Override
            protected Page createPage(final int page) throws Exception
            {

               Page originalPage = super.createPage(page);

               return new FailingPage(originalPage);
            }

         }

      }

      class FailingPage implements Page
      {
         Page delegatedPage;

         /**
          * @throws Exception
          * @see org.jboss.messaging.core.paging.Page#close()
          */
         public void close() throws Exception
         {
            delegatedPage.close();
         }

         /**
          * @throws Exception
          * @see org.jboss.messaging.core.paging.Page#delete()
          */
         public void delete() throws Exception
         {
            // We want the system to fail
            System.out.println("Crash");
            System.out.flush(); // System.exit may not let the System.out to be seen if flush is not called
            System.exit(1);
         }

         /**
          * @return
          * @see org.jboss.messaging.core.paging.Page#getNumberOfMessages()
          */
         public int getNumberOfMessages()
         {
            return delegatedPage.getNumberOfMessages();
         }

         /**
          * @return
          * @see org.jboss.messaging.core.paging.Page#getPageId()
          */
         public int getPageId()
         {
            return delegatedPage.getPageId();
         }

         /**
          * @return
          * @see org.jboss.messaging.core.paging.Page#getSize()
          */
         public int getSize()
         {
            return delegatedPage.getSize();
         }

         /**
          * @throws Exception
          * @see org.jboss.messaging.core.paging.Page#open()
          */
         public void open() throws Exception
         {
            delegatedPage.open();
         }

         /**
          * @return
          * @throws Exception
          * @see org.jboss.messaging.core.paging.Page#read()
          */
         public List<PagedMessage> read() throws Exception
         {
            return delegatedPage.read();
         }

         /**
          * @throws Exception
          * @see org.jboss.messaging.core.paging.Page#sync()
          */
         public void sync() throws Exception
         {
            delegatedPage.sync();
         }

         /**
          * @param message
          * @throws Exception
          * @see org.jboss.messaging.core.paging.Page#write(org.jboss.messaging.core.paging.PagedMessage)
          */
         public void write(final PagedMessage message) throws Exception
         {
            delegatedPage.write(message);
         }

         public FailingPage(final Page delegatePage)
         {
            delegatedPage = delegatePage;
         }
      }

   }

}
