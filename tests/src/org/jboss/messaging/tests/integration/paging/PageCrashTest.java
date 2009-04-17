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

package org.jboss.messaging.tests.integration.paging;

import java.io.File;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PagedMessage;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.impl.PagingManagerImpl;
import org.jboss.messaging.core.paging.impl.PagingStoreFactoryNIO;
import org.jboss.messaging.core.paging.impl.PagingStoreImpl;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.impl.JBMSecurityManagerImpl;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.OrderedExecutorFactory;
import org.jboss.messaging.utils.SimpleString;

/**
 * This test will make sure that a failing depage won't cause duplicated messages
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Jan 7, 2009 6:19:43 PM
 *
 *
 */
public class PageCrashTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   public static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCrashDuringDeleteFile() throws Exception
   {
      pageAndFail();

      File pageDir = new File(getPageDir());

      File directories[] = pageDir.listFiles();

      assertEquals(1, directories.length);

      // When depage happened, a new empty page was supposed to be opened, what will create 3 files
      assertEquals("Missing a file, supposed to have address.txt, 1st page and 2nd page",
                   3,
                   directories[0].list().length);

      Configuration config = createDefaultConfig();

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      MessagingServer messagingService = createServer(true, config, new HashMap<String, AddressSettings>());

      messagingService.start();

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         assertNull(consumer.receive(200));

         session.close();
      }
      finally
      {
         messagingService.stop();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /** This method will leave garbage on paging. 
    *  It will not delete page files as if the server crashed right after commit, 
    *  and before removing the file*/
   private void pageAndFail() throws Exception
   {
      clearData();
      Configuration config = createDefaultConfig();

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      MessagingServer server = newMessagingServer(config);

      server.start();

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         // Making it synchronous, just because we want to stop sending messages as soon as the page-store becomes in
         // page mode
         // and we could only guarantee that by setting it to synchronous
         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         ClientMessage message = null;

         message = session.createClientMessage(true);
         message.getBody().writeBytes(new byte[1024]);

         PagingStore store = server.getPostOffice().getPagingManager().getPageStore(ADDRESS);

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

         assertTrue(server.getPostOffice().getPagingManager().getGlobalSize() > 0);

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

         assertEquals(0, server.getPostOffice().getPagingManager().getGlobalSize());

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private MessagingServer newMessagingServer(final Configuration configuration)
   {
      JBMSecurityManager securityManager = new JBMSecurityManagerImpl();

      MessagingServer server = new FailingMessagingServerImpl(configuration, securityManager);

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(configuration.getPagingGlobalWatermarkSize());

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   // Inner classes -------------------------------------------------

   /** This is hacking MessagingServerImpl, 
    *  to make sure the server will fail right 
    *  before the page-file was removed */
   class FailingMessagingServerImpl extends MessagingServerImpl
   {
      FailingMessagingServerImpl(final Configuration config, final JBMSecurityManager securityManager)
      {
         super(config, new FakeMBean(), securityManager);
      }

      @Override
      protected PagingManager createPagingManager()
      {
         return new PagingManagerImpl(new FailurePagingStoreFactoryNIO(super.getConfiguration().getPagingDirectory(),
                                                                       super.getConfiguration().getPagingMaxThreads()),
                                      super.getStorageManager(),
                                      super.getAddressSettingsRepository(),
                                      super.getConfiguration().getPagingMaxGlobalSizeBytes(),
                                      super.getConfiguration().getPagingGlobalWatermarkSize(),
                                      super.getConfiguration().isJournalSyncNonTransactional(),
                                      false);
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
         public synchronized PagingStore newStore(final SimpleString destinationName, final AddressSettings settings) throws Exception
         {
            Field factoryField = PagingStoreFactoryNIO.class.getDeclaredField("executorFactory");
            factoryField.setAccessible(true);

            OrderedExecutorFactory factory = (org.jboss.messaging.utils.OrderedExecutorFactory)factoryField.get(this);
            return new FailingPagingStore(destinationName, settings, factory.getExecutor());
         }

         // Package protected ---------------------------------------------

         // Protected -----------------------------------------------------

         // Private -------------------------------------------------------

         // Inner classes -------------------------------------------------
         class FailingPagingStore extends PagingStoreImpl
         {

            /**
             * @param storeName
             * @param addressSettings
             * @param executor
             */
            public FailingPagingStore(final SimpleString storeName,
                                      final AddressSettings addressSettings,
                                      final Executor executor)
            {
               super(getPostOffice().getPagingManager(),
                     getStorageManager(),
                     getPostOffice(),
                     null,
                     FailurePagingStoreFactoryNIO.this,
                     storeName,
                     addressSettings,
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
            // This will let the file stay, simulating a system failure
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

   class FakeMBean implements MBeanServer
   {

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#addNotificationListener(javax.management.ObjectName, javax.management.NotificationListener, javax.management.NotificationFilter, java.lang.Object)
       */
      public void addNotificationListener(ObjectName name,
                                          NotificationListener listener,
                                          NotificationFilter filter,
                                          Object handback) throws InstanceNotFoundException
      {

      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#addNotificationListener(javax.management.ObjectName, javax.management.ObjectName, javax.management.NotificationFilter, java.lang.Object)
       */
      public void addNotificationListener(ObjectName name,
                                          ObjectName listener,
                                          NotificationFilter filter,
                                          Object handback) throws InstanceNotFoundException
      {

      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#createMBean(java.lang.String, javax.management.ObjectName)
       */
      public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException,
                                                                          InstanceAlreadyExistsException,
                                                                          MBeanRegistrationException,
                                                                          MBeanException,
                                                                          NotCompliantMBeanException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#createMBean(java.lang.String, javax.management.ObjectName, javax.management.ObjectName)
       */
      public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName) throws ReflectionException,
                                                                                                 InstanceAlreadyExistsException,
                                                                                                 MBeanRegistrationException,
                                                                                                 MBeanException,
                                                                                                 NotCompliantMBeanException,
                                                                                                 InstanceNotFoundException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#createMBean(java.lang.String, javax.management.ObjectName, java.lang.Object[], java.lang.String[])
       */
      public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature) throws ReflectionException,
                                                                                                               InstanceAlreadyExistsException,
                                                                                                               MBeanRegistrationException,
                                                                                                               MBeanException,
                                                                                                               NotCompliantMBeanException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#createMBean(java.lang.String, javax.management.ObjectName, javax.management.ObjectName, java.lang.Object[], java.lang.String[])
       */
      public ObjectInstance createMBean(String className,
                                        ObjectName name,
                                        ObjectName loaderName,
                                        Object[] params,
                                        String[] signature) throws ReflectionException,
                                                           InstanceAlreadyExistsException,
                                                           MBeanRegistrationException,
                                                           MBeanException,
                                                           NotCompliantMBeanException,
                                                           InstanceNotFoundException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#deserialize(javax.management.ObjectName, byte[])
       */
      public ObjectInputStream deserialize(ObjectName name, byte[] data) throws InstanceNotFoundException,
                                                                        OperationsException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#deserialize(java.lang.String, byte[])
       */
      public ObjectInputStream deserialize(String className, byte[] data) throws OperationsException,
                                                                         ReflectionException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#deserialize(java.lang.String, javax.management.ObjectName, byte[])
       */
      public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data) throws InstanceNotFoundException,
                                                                                                OperationsException,
                                                                                                ReflectionException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getAttribute(javax.management.ObjectName, java.lang.String)
       */
      public Object getAttribute(ObjectName name, String attribute) throws MBeanException,
                                                                   AttributeNotFoundException,
                                                                   InstanceNotFoundException,
                                                                   ReflectionException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getAttributes(javax.management.ObjectName, java.lang.String[])
       */
      public AttributeList getAttributes(ObjectName name, String[] attributes) throws InstanceNotFoundException,
                                                                              ReflectionException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getClassLoader(javax.management.ObjectName)
       */
      public ClassLoader getClassLoader(ObjectName loaderName) throws InstanceNotFoundException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getClassLoaderFor(javax.management.ObjectName)
       */
      public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getClassLoaderRepository()
       */
      public ClassLoaderRepository getClassLoaderRepository()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getDefaultDomain()
       */
      public String getDefaultDomain()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getDomains()
       */
      public String[] getDomains()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getMBeanCount()
       */
      public Integer getMBeanCount()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getMBeanInfo(javax.management.ObjectName)
       */
      public MBeanInfo getMBeanInfo(ObjectName name) throws InstanceNotFoundException,
                                                    IntrospectionException,
                                                    ReflectionException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#getObjectInstance(javax.management.ObjectName)
       */
      public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#instantiate(java.lang.String)
       */
      public Object instantiate(String className) throws ReflectionException, MBeanException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#instantiate(java.lang.String, javax.management.ObjectName)
       */
      public Object instantiate(String className, ObjectName loaderName) throws ReflectionException,
                                                                        MBeanException,
                                                                        InstanceNotFoundException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#instantiate(java.lang.String, java.lang.Object[], java.lang.String[])
       */
      public Object instantiate(String className, Object[] params, String[] signature) throws ReflectionException,
                                                                                      MBeanException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#instantiate(java.lang.String, javax.management.ObjectName, java.lang.Object[], java.lang.String[])
       */
      public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature) throws ReflectionException,
                                                                                                             MBeanException,
                                                                                                             InstanceNotFoundException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#invoke(javax.management.ObjectName, java.lang.String, java.lang.Object[], java.lang.String[])
       */
      public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature) throws InstanceNotFoundException,
                                                                                                      MBeanException,
                                                                                                      ReflectionException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#isInstanceOf(javax.management.ObjectName, java.lang.String)
       */
      public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException
      {

         return false;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#isRegistered(javax.management.ObjectName)
       */
      public boolean isRegistered(ObjectName name)
      {

         return false;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#queryMBeans(javax.management.ObjectName, javax.management.QueryExp)
       */
      public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#queryNames(javax.management.ObjectName, javax.management.QueryExp)
       */
      public Set<ObjectName> queryNames(ObjectName name, QueryExp query)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#registerMBean(java.lang.Object, javax.management.ObjectName)
       */
      public ObjectInstance registerMBean(Object object, ObjectName name) throws InstanceAlreadyExistsException,
                                                                         MBeanRegistrationException,
                                                                         NotCompliantMBeanException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#removeNotificationListener(javax.management.ObjectName, javax.management.ObjectName)
       */
      public void removeNotificationListener(ObjectName name, ObjectName listener) throws InstanceNotFoundException,
                                                                                  ListenerNotFoundException
      {

      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#removeNotificationListener(javax.management.ObjectName, javax.management.NotificationListener)
       */
      public void removeNotificationListener(ObjectName name, NotificationListener listener) throws InstanceNotFoundException,
                                                                                            ListenerNotFoundException
      {

      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#removeNotificationListener(javax.management.ObjectName, javax.management.ObjectName, javax.management.NotificationFilter, java.lang.Object)
       */
      public void removeNotificationListener(ObjectName name,
                                             ObjectName listener,
                                             NotificationFilter filter,
                                             Object handback) throws InstanceNotFoundException,
                                                             ListenerNotFoundException
      {

      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#removeNotificationListener(javax.management.ObjectName, javax.management.NotificationListener, javax.management.NotificationFilter, java.lang.Object)
       */
      public void removeNotificationListener(ObjectName name,
                                             NotificationListener listener,
                                             NotificationFilter filter,
                                             Object handback) throws InstanceNotFoundException,
                                                             ListenerNotFoundException
      {

      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#setAttribute(javax.management.ObjectName, javax.management.Attribute)
       */
      public void setAttribute(ObjectName name, Attribute attribute) throws InstanceNotFoundException,
                                                                    AttributeNotFoundException,
                                                                    InvalidAttributeValueException,
                                                                    MBeanException,
                                                                    ReflectionException
      {

      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#setAttributes(javax.management.ObjectName, javax.management.AttributeList)
       */
      public AttributeList setAttributes(ObjectName name, AttributeList attributes) throws InstanceNotFoundException,
                                                                                   ReflectionException
      {

         return null;
      }

      /* (non-Javadoc)
       * @see javax.management.MBeanServer#unregisterMBean(javax.management.ObjectName)
       */
      public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException
      {

      }

   }

   // Inner classes -------------------------------------------------

}
