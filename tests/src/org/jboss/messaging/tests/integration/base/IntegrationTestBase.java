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
   
   protected static final String ACCEPTOR_FACTORY = "org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory";
   protected static final String CONNECTOR_FACTORY = "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory";
   
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


   protected MessagingService createService(Configuration configuration, Map<String, QueueSettings> settings)
   {
      TransportConfiguration transportConfig = new TransportConfiguration(ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      MessagingService service = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      
      for (Map.Entry<String, QueueSettings> setting: settings.entrySet())
      {
         service.getServer().getQueueSettingsRepository().addMatch(setting.getKey(), setting.getValue());
      }
   
      
      return service;
   }

   protected MessagingService createService()
   {
      return createService(createDefaultConfig(), new HashMap<String, QueueSettings>());
   }


   protected Configuration createDefaultConfig()
   {
      Configuration configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setPagingDirectory(pageDir);
      
      return configuration;
   }


   protected ClientSessionFactory createFactory()
   {
      return new ClientSessionFactoryImpl(new TransportConfiguration(CONNECTOR_FACTORY));
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
