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

   protected String baseDir = System.getProperty("java.io.tmpdir", "/tmp") + "/jbm-unit-test";

   protected String journalDir = baseDir + "/journal";

   protected String bindingsDir = baseDir + "/bindings";

   protected String pageDir = baseDir + "/page";

   protected String largeMessagesDir = baseDir + "/large-msg";

   protected String clientLargeMessagesDir = baseDir + "/client-large-msg";

   protected String temporaryDir = baseDir + "/temporary";

   protected MessagingService messagingService;

   // Static --------------------------------------------------------
   private static final Logger log = Logger.getLogger(ServiceTestBase.class);

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void clearData()
   {
      deleteAndCreateDir(journalDir);
      deleteAndCreateDir(bindingsDir);
      deleteAndCreateDir(pageDir);
      deleteAndCreateDir(largeMessagesDir);
      deleteAndCreateDir(clientLargeMessagesDir);
      deleteAndCreateDir(temporaryDir);
   }

   protected void deleteData()
   {
      log.info("deleting directory " + baseDir);
      deleteDirectory(new File(baseDir));
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
         service = MessagingServiceImpl.newNioStorageMessagingServer(configuration,
                                                                     journalDir,
                                                                     bindingsDir,
                                                                     largeMessagesDir);
      }
      else
      {
         service = MessagingServiceImpl.newNullStorageMessagingServer(configuration);
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

   protected Configuration createDefaultConfig()
   {
      return createDefaultConfig(false);
   }

   protected Configuration createDefaultConfig(final boolean netty)
   {
      if (netty)
      {
         return createDefaultConfig(INVM_ACCEPTOR_FACTORY, NETTY_ACCEPTOR_FACTORY);
      }
      else
      {
         return createDefaultConfig(INVM_ACCEPTOR_FACTORY);
      }
      
   }

   protected Configuration createDefaultConfig(final String... acceptors)
   {
      Configuration configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setJournalFileSize(100 * 1024);
      configuration.setPagingDirectory(pageDir);
      configuration.setLargeMessagesDirectory(largeMessagesDir);

      configuration.getAcceptorConfigurations().clear();

      for (String acceptor : acceptors)
      {
         TransportConfiguration transportConfig = new TransportConfiguration(acceptor);
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
