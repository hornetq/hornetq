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


package org.jboss.messaging.tests.integration.base;

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
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.jms.client.JBossBytesMessage;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * Base class with basic utilities on starting up a basic server
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class IntegrationTestBase extends UnitTestCase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected static final String INVM_ACCEPTOR_FACTORY = InVMAcceptorFactory.class.getCanonicalName();
   protected static final String INVM_CONNECTOR_FACTORY = InVMConnectorFactory.class.getCanonicalName();
   
   protected static final String NETTY_ACCEPTOR_FACTORY = NettyAcceptorFactory.class.getCanonicalName();
   protected static final String NETTY_CONNECTOR_FACTORY = NettyConnectorFactory.class.getCanonicalName();
   
   protected String journalDir = System.getProperty("java.io.tmpdir", "/tmp") + "/integration-test/journal";
   protected String bindingsDir = System.getProperty("java.io.tmpdir", "/tmp") + "/integration-test/bindings";
   protected String pageDir = System.getProperty("java.io.tmpdir", "/tmp") + "/integration-test/page";
   protected MessagingService messagingService;

   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void clearData()
   {
      File file = new File(journalDir);
      File file2 = new File(bindingsDir);
      File file3 = new File(pageDir);
      deleteDirectory(file);
      file.mkdirs();
      deleteDirectory(file2);
      file2.mkdirs();
      deleteDirectory(file3);
      file3.mkdirs();
   }


   protected MessagingService createService(boolean realFiles, boolean netty, Configuration configuration, Map<String, QueueSettings> settings)
   {
      TransportConfiguration transportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      
      if (netty)
      {
         configuration.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY));
      }
      
      MessagingService service;
      
      if (realFiles)
      {
         service = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      }
      else
      {
         service = MessagingServiceImpl.newNullStorageMessagingServer(configuration);
      }
         
      
      for (Map.Entry<String, QueueSettings> setting: settings.entrySet())
      {
         service.getServer().getQueueSettingsRepository().addMatch(setting.getKey(), setting.getValue());
      }
   
      
      return service;
   }

   protected MessagingService createService(boolean realFiles)
   {
      return createService(realFiles, false, createDefaultConfig(), new HashMap<String, QueueSettings>());
   }


   protected Configuration createDefaultConfig()
   {
      Configuration configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setPagingDirectory(pageDir);
      
      return configuration;
   }


   protected ClientSessionFactory createInVMFactory()
   {
      return new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
   }
   
   protected ClientSessionFactory createNettyFactory()
   {
      return new ClientSessionFactoryImpl(new TransportConfiguration(NETTY_CONNECTOR_FACTORY));
   }
   
   protected ClientMessage createTextMessage(ClientSession session, String s)
   {
      return createTextMessage(session, s, true);
   }

   protected ClientMessage createTextMessage(ClientSession session, String s, boolean durable)
   {
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, durable, 0, System.currentTimeMillis(), (byte) 1);
      message.getBody().putString(s);
      message.getBody().flip();
      return message;
   }

   protected ClientMessage createBytesMessage(ClientSession session, byte[] b, boolean durable)
   {
      ClientMessage message = session.createClientMessage(JBossBytesMessage.TYPE, durable, 0, System.currentTimeMillis(), (byte) 1);
      message.getBody().putBytes(b);
      message.getBody().flip();
      return message;
   }

   
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}
