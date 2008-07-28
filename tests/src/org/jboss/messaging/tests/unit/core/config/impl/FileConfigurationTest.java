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

package org.jboss.messaging.tests.unit.core.config.impl;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.FileConfiguration;
import org.jboss.messaging.core.remoting.TransportType;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class FileConfigurationTest extends ConfigurationImplTest
{
   public void testDefaults()
   {
      //Check they match the values from the test file
      assertEquals(true, conf.isClustered());
      assertEquals(12345, conf.getScheduledThreadPoolMaxSize());
      assertEquals("blahhost", conf.getHost());
      assertEquals(TransportType.HTTP, conf.getTransport());
      assertEquals(6540, conf.getPort());
      assertEquals(5423, conf.getSecurityInvalidationInterval());
      assertEquals(false, conf.isRequireDestinations());
      assertEquals(false, conf.isSecurityEnabled());
      assertEquals(true, conf.isSSLEnabled());
      assertEquals("blahstore", conf.getKeyStorePath());
      assertEquals("wibble123", conf.getKeyStorePassword());
      assertEquals("blahtruststore", conf.getTrustStorePath());
      assertEquals("eek123", conf.getTrustStorePassword());
      assertEquals("somedir", conf.getBindingsDirectory());
      assertEquals(false, conf.isCreateBindingsDir());
      assertEquals("somedir2", conf.getJournalDirectory());
      assertEquals(false, conf.isCreateJournalDir());
      assertEquals("NIO", conf.getJournalType().toString());
      assertEquals(false, conf.isJournalSyncTransactional());
      assertEquals(true, conf.isJournalSyncNonTransactional());
      assertEquals(12345678, conf.getJournalFileSize());
      assertEquals(100, conf.getJournalMinFiles());      
      assertEquals(56546, conf.getJournalMaxAIO());
      
      assertEquals(false, conf.getConnectionParams().isInVMOptimisationEnabled());
      assertEquals(7654, conf.getConnectionParams().getCallTimeout());
      assertEquals(false, conf.getConnectionParams().isTcpNoDelay());
      assertEquals(987654, conf.getConnectionParams().getTcpReceiveBufferSize());
      assertEquals(2345676, conf.getConnectionParams().getTcpSendBufferSize());
      assertEquals(123123, conf.getConnectionParams().getPingInterval());
      assertEquals(2, conf.getInterceptorClassNames().size());
      assertTrue(conf.getInterceptorClassNames().contains("org.jboss.messaging.tests.unit.core.config.impl.TestInterceptor1"));
      assertTrue(conf.getInterceptorClassNames().contains("org.jboss.messaging.tests.unit.core.config.impl.TestInterceptor2"));
      assertEquals(2, conf.getAcceptorFactoryClassNames().size());
      assertTrue(conf.getAcceptorFactoryClassNames().contains("org.jboss.messaging.tests.unit.core.config.impl.TestAcceptorFactory1"));
      assertTrue(conf.getAcceptorFactoryClassNames().contains("org.jboss.messaging.tests.unit.core.config.impl.TestAcceptorFactory2")); 
   }
   
   public void testSetGetConfigurationURL()
   {
      final String file = "ghuuhhu";
      
      FileConfiguration fc = new FileConfiguration();
      
      fc.setConfigurationUrl(file);
      
      assertEquals(file, fc.getConfigurationUrl());
      
   }

   // Protected ---------------------------------------------------------------------------------------------
   
   protected Configuration createConfiguration() throws Exception
   {
      FileConfiguration fc = new FileConfiguration();
      
      fc.setConfigurationUrl("ConfigurationTest-config.xml");
      
      fc.start();
      
      return fc;
   }

}
