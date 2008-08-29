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

import java.util.Map;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.FileConfiguration;

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
      assertEquals(true, conf.isBackup());
      assertEquals(12345, conf.getScheduledThreadPoolMaxSize());    
      assertEquals(5423, conf.getSecurityInvalidationInterval());
      assertEquals(false, conf.isRequireDestinations());
      assertEquals(false, conf.isSecurityEnabled());
      assertEquals(7654, conf.getCallTimeout());
      assertEquals(543, conf.getPacketConfirmationBatchSize());
      assertEquals(6543, conf.getConnectionScanPeriod());
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
      
      assertEquals(2, conf.getInterceptorClassNames().size());
      assertTrue(conf.getInterceptorClassNames().contains("org.jboss.messaging.tests.unit.core.config.impl.TestInterceptor1"));
      assertTrue(conf.getInterceptorClassNames().contains("org.jboss.messaging.tests.unit.core.config.impl.TestInterceptor2"));
      
      assertNotNull(conf.getBackupConnectorConfiguration());
      assertEquals("org.jboss.messaging.tests.unit.core.config.impl.TestConnectorFactory1", conf.getBackupConnectorConfiguration().getFactoryClassName());
      Map<String, Object> params = conf.getBackupConnectorConfiguration().getParams();
      assertNotNull(params);
      Object obj = params.get("c_mykey1");
      assertNotNull(obj);
      assertTrue(obj instanceof String);
      {
         String s = (String)obj;
         assertEquals("c_foovalue1", s);
      }
      
      obj = params.get("c_mykey2");
      assertNotNull(obj);
      assertTrue(obj instanceof Long);
      {
         Long l = (Long)obj;
         assertEquals(6000l, l.longValue());
      }
      
      obj = params.get("c_mykey3");
      assertNotNull(obj);
      assertTrue(obj instanceof Integer);
      {
         Integer i = (Integer)obj;
         assertEquals(60, i.intValue());
      }
      
      obj = params.get("c_mykey4");
      assertNotNull(obj);
      assertTrue(obj instanceof String);
      {
         String s = (String)obj;
         assertEquals("c_foovalue4", s);
      }
      
      assertEquals(2, conf.getAcceptorConfigurations().size());
      for (TransportConfiguration info: conf.getAcceptorConfigurations())
      {
         if (info.getFactoryClassName().equals("org.jboss.messaging.tests.unit.core.config.impl.TestAcceptorFactory1"))
         {
            params = info.getParams();
            
            obj = params.get("a_mykey1");
            assertNotNull(obj);
            assertTrue(obj instanceof String);
            {
               String s = (String)obj;
               assertEquals("a_foovalue1", s);
            }
            
            obj = params.get("a_mykey2");
            assertNotNull(obj);
            assertTrue(obj instanceof Long);
            {
               Long l = (Long)obj;
               assertEquals(1234567l, l.longValue());
            }
            
            obj = params.get("a_mykey3");
            assertNotNull(obj);
            assertTrue(obj instanceof Integer);
            {
               Integer i = (Integer)obj;
               assertEquals(123, i.intValue());
            }
            
            obj = params.get("a_mykey4");
            assertNotNull(obj);
            assertTrue(obj instanceof String);
            {
               String s = (String)obj;
               assertEquals("a_foovalue4", s);
            }
         }
         else if (info.getFactoryClassName().equals("org.jboss.messaging.tests.unit.core.config.impl.TestAcceptorFactory2"))
         {
            params = info.getParams();
            
            obj = params.get("b_mykey1");
            assertNotNull(obj);
            assertTrue(obj instanceof String);
            {
               String s = (String)obj;
               assertEquals("b_foovalue1", s);
            }
            
            obj = params.get("b_mykey2");
            assertNotNull(obj);
            assertTrue(obj instanceof Long);
            {
               Long l = (Long)obj;
               assertEquals(7654321l, l.longValue());
            }
            
            obj = params.get("b_mykey3");
            assertNotNull(obj);
            assertTrue(obj instanceof Integer);
            {
               Integer i = (Integer)obj;
               assertEquals(321, i.intValue());
            }
            
            obj = params.get("b_mykey4");
            assertNotNull(obj);
            assertTrue(obj instanceof String);
            {
               String s = (String)obj;
               assertEquals("b_foovalue4", s);
            }
         }
         else
         {
            fail("Invalid factory class");
         }
      }
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
