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
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.config.impl.FileConfiguration;

/**
 * 
 * A FileConfigurationTest2
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FileConfigurationTest2 extends ConfigurationImplTest
{
   public void testDefaults()
   {
      assertEquals(ConfigurationImpl.DEFAULT_CLUSTERED, conf.isClustered());
      assertEquals(ConfigurationImpl.DEFAULT_BACKUP, conf.isBackup());
      assertEquals(ConfigurationImpl.DEFAULT_QUEUE_ACTIVATION_TIMEOUT, conf.getQueueActivationTimeout());
      assertEquals(ConfigurationImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, conf.getScheduledThreadPoolMaxSize());
      assertEquals(ConfigurationImpl.DEFAULT_SECURITY_INVALIDATION_INTERVAL, conf.getSecurityInvalidationInterval());
      assertEquals(ConfigurationImpl.DEFAULT_REQUIRE_DESTINATIONS, conf.isRequireDestinations());
      assertEquals(ConfigurationImpl.DEFAULT_SECURITY_ENABLED, conf.isSecurityEnabled());
      assertEquals(ConfigurationImpl.DEFAULT_CONNECTION_SCAN_PERIOD, conf.getConnectionScanPeriod());
      assertEquals(ConfigurationImpl.DEFAULT_BINDINGS_DIRECTORY, conf.getBindingsDirectory());
      assertEquals(ConfigurationImpl.DEFAULT_CREATE_BINDINGS_DIR, conf.isCreateBindingsDir());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_DIR, conf.getJournalDirectory());
      assertEquals(ConfigurationImpl.DEFAULT_CREATE_JOURNAL_DIR, conf.isCreateJournalDir());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_TYPE.toString(), conf.getJournalType().toString());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_TRANSACTIONAL, conf.isJournalSyncTransactional());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL, conf.isJournalSyncNonTransactional());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE, conf.getJournalFileSize());
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MIN_FILES, conf.getJournalMinFiles());      
      assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_MAX_AIO, conf.getJournalMaxAIO());
      
      assertEquals(0, conf.getInterceptorClassNames().size());
      assertEquals(0, conf.getAcceptorConfigurations().size());      
   }
   
   // Protected ---------------------------------------------------------------------------------------------
   
   protected Configuration createConfiguration() throws Exception
   {
      FileConfiguration fc = new FileConfiguration();
      
      fc.setConfigurationUrl("ConfigurationTest-config2.xml");
      
      fc.start();
      
      return fc;
   }

}

