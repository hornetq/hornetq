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

package org.jboss.messaging.tests.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.jms.client.JBossBytesMessage;
import org.jboss.messaging.jms.client.JBossTextMessage;

/**
 * 
 * Base class with basic utilities on starting up a basic server
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class ServiceTestBase extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   
   protected static final String INVM_ACCEPTOR_FACTORY = InVMAcceptorFactory.class.getCanonicalName();

   protected static final String INVM_CONNECTOR_FACTORY = InVMConnectorFactory.class.getCanonicalName();

   protected static final String NETTY_ACCEPTOR_FACTORY = NettyAcceptorFactory.class.getCanonicalName();

   protected static final String NETTY_CONNECTOR_FACTORY = NettyConnectorFactory.class.getCanonicalName();

   // Static --------------------------------------------------------
   private final Logger log = Logger.getLogger(this.getClass());

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------


   protected void clearData()
   {
      clearData(getTestDir());
   }

   
   protected void clearData(String testDir)
   {
      deleteAndCreateDir(getJournalDir(testDir));
      deleteAndCreateDir(getBindingsDir(testDir));
      deleteAndCreateDir(getPageDir(testDir));
      deleteAndCreateDir(getLargeMessagesDir(testDir));
      deleteAndCreateDir(getClientLargeMessagesDir(testDir));
      deleteAndCreateDir(getTemporaryDir(testDir));
   }

   protected void deleteAndCreateDir(String directory)
   {
      File file = new File(directory);
      deleteDirectory(file);
      file.mkdirs();
   }

   protected MessagingService createService(final boolean realFiles,
                                            final Configuration configuration,
                                            final Map<String, QueueSettings> settings)
   {

      MessagingService service;

      if (realFiles)
      {
         service = MessagingServiceImpl.newMessagingService(configuration);
      }
      else
      {
         service = MessagingServiceImpl.newNullStorageMessagingService(configuration);
      }

      for (Map.Entry<String, QueueSettings> setting : settings.entrySet())
      {
         service.getServer().getQueueSettingsRepository().addMatch(setting.getKey(), setting.getValue());
      }

      return service;
   }

   protected MessagingService createService(final boolean realFiles)
   {
      return createService(realFiles, createDefaultConfig(), new HashMap<String, QueueSettings>());
   }

   protected MessagingService createService(final boolean realFiles, final Configuration configuration)
   {
      return createService(realFiles, configuration, new HashMap<String, QueueSettings>());
   }
   
   protected MessagingService createClusteredServiceWithParams(final int index, final boolean realFiles, final Map<String, Object> params)
   {
      return createService(realFiles, createClusteredDefaultConfig(index, params, INVM_ACCEPTOR_FACTORY), new HashMap<String, QueueSettings>());
   }
   
   protected Configuration createDefaultConfig()
   {
      return createDefaultConfig(false);
   }

   protected Configuration createDefaultConfig(final boolean netty)
   {
      if (netty)
      {
         return createDefaultConfig(new HashMap<String, Object>(), INVM_ACCEPTOR_FACTORY, NETTY_ACCEPTOR_FACTORY);
      }
      else
      {
         return createDefaultConfig(new HashMap<String, Object>(), INVM_ACCEPTOR_FACTORY);
      }      
   }
   
   protected Configuration createClusteredDefaultConfig(final int index, final Map<String, Object> params, final String... acceptors)
   {
      Configuration config = createDefaultConfig(index, params, acceptors);
      
      config.setClustered(true);
      
      return config;
   }
   
   protected Configuration createDefaultConfig(int index, final Map<String, Object> params, final String... acceptors)
   {
      Configuration configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir(index));
      configuration.setJournalMinFiles(2);
      configuration.setJournalDirectory(getJournalDir(index));
      configuration.setJournalFileSize(100 * 1024);
      configuration.setPagingDirectory(getPageDir(index));
      configuration.setLargeMessagesDirectory(getLargeMessagesDir(index));

      configuration.getAcceptorConfigurations().clear();

      for (String acceptor : acceptors)
      {
         TransportConfiguration transportConfig = new TransportConfiguration(acceptor, params);
         configuration.getAcceptorConfigurations().add(transportConfig);
      }

      return configuration;
   }

   protected Configuration createDefaultConfig(final Map<String, Object> params, final String... acceptors)
   {
      Configuration configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir());
      configuration.setJournalMinFiles(2);
      configuration.setJournalDirectory(getJournalDir());
      configuration.setJournalFileSize(100 * 1024);
      configuration.setPagingDirectory(getPageDir());
      configuration.setLargeMessagesDirectory(getLargeMessagesDir());

      configuration.getAcceptorConfigurations().clear();

      for (String acceptor : acceptors)
      {
         TransportConfiguration transportConfig = new TransportConfiguration(acceptor, params);
         configuration.getAcceptorConfigurations().add(transportConfig);
      }

      return configuration;
   }

   protected ClientSessionFactory createInVMFactory()
   {
      return createFactory(INVM_CONNECTOR_FACTORY);
   }

   protected ClientSessionFactory createNettyFactory()
   {
      return createFactory(NETTY_CONNECTOR_FACTORY);
   }

   protected ClientSessionFactory createFactory(final String connectorClass)
   {
      return new ClientSessionFactoryImpl(new TransportConfiguration(connectorClass),
                                          null);

   }

   protected ClientMessage createTextMessage(final ClientSession session, final String s)
   {
      return createTextMessage(session, s, true);
   }

   protected ClientMessage createTextMessage(final ClientSession session, final String s, final boolean durable)
   {
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                          durable,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBody().putString(s);
      message.getBody().flip();
      return message;
   }

   protected ClientMessage createBytesMessage(final ClientSession session, final byte[] b, final boolean durable)
   {
      ClientMessage message = session.createClientMessage(JBossBytesMessage.TYPE,
                                                          durable,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBody().putBytes(b);
      message.getBody().flip();
      return message;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
