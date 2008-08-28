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

package org.jboss.messaging.core.config.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.config.AcceptorInfo;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.server.JournalType;

/**
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConfigurationImpl implements Configuration
{
   // Constants ------------------------------------------------------------------------------
   
   private static final long serialVersionUID = 4077088945050267843L;
  
   public static final boolean DEFAULT_CLUSTERED = false;
   
   public static final boolean DEFAULT_BACKUP = false;
   
   public static final int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 30;
   
   public static final long DEFAULT_SECURITY_INVALIDATION_INTERVAL = 10000;
   
   public static final boolean DEFAULT_REQUIRE_DESTINATIONS = false;
   
   public static final boolean DEFAULT_SECURITY_ENABLED = true;
   
   public static final boolean DEFAULT_JMX_MANAGEMENT_ENABLED = true;
     
   public static final int DEFAULT_CALL_TIMEOUT = 30000;
   
   public static final int DEFAULT_PACKET_CONFIRMATION_BATCH_SIZE = 100;
   
   public static final long DEFAULT_CONNECTION_SCAN_PERIOD = 1000;
   
   public static final String DEFAULT_BINDINGS_DIRECTORY = "data/bindings";
   
   public static final boolean DEFAULT_CREATE_BINDINGS_DIR = true;
   
   public static final String DEFAULT_JOURNAL_DIR = "data/journal";
   
   public static final boolean DEFAULT_CREATE_JOURNAL_DIR = true;
   
   public static final JournalType DEFAULT_JOURNAL_TYPE = JournalType.ASYNCIO;
   
   public static final boolean DEFAULT_JOURNAL_SYNC_TRANSACTIONAL = true;
   
   public static final boolean DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL = false;
   
   public static final int DEFAULT_JOURNAL_FILE_SIZE = 10485760;
   
   public static final int DEFAULT_JOURNAL_MIN_FILES = 10;
   
   public static final int DEFAULT_JOURNAL_MAX_AIO = 5000;
   
   
   
   // Attributes -----------------------------------------------------------------------------
      
   protected boolean clustered = DEFAULT_CLUSTERED;
   
   protected boolean backup = DEFAULT_BACKUP;
      
   protected int scheduledThreadPoolMaxSize = DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
   
   protected long securityInvalidationInterval = DEFAULT_SECURITY_INVALIDATION_INTERVAL;

   protected boolean requireDestinations = DEFAULT_REQUIRE_DESTINATIONS;
   
   protected boolean securityEnabled = DEFAULT_SECURITY_ENABLED;

   protected boolean jmxManagementEnabled = DEFAULT_JMX_MANAGEMENT_ENABLED;
   
   protected long callTimeout = DEFAULT_CALL_TIMEOUT;
   
   protected int packetConfirmationBatchSize = DEFAULT_PACKET_CONFIRMATION_BATCH_SIZE;
   
   protected long connectionScanPeriod = DEFAULT_CONNECTION_SCAN_PERIOD;
   
         
   // Journal related attributes
   
   protected String bindingsDirectory = DEFAULT_BINDINGS_DIRECTORY;
   
   protected boolean createBindingsDir = DEFAULT_CREATE_BINDINGS_DIR;
   
   protected String journalDirectory = DEFAULT_JOURNAL_DIR;
   
   protected boolean createJournalDir = DEFAULT_CREATE_JOURNAL_DIR;
   
   public JournalType journalType = DEFAULT_JOURNAL_TYPE;
   
   protected boolean journalSyncTransactional = DEFAULT_JOURNAL_SYNC_TRANSACTIONAL;
   
   protected boolean journalSyncNonTransactional = DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL;
   
   protected int journalFileSize = DEFAULT_JOURNAL_FILE_SIZE;
   
   protected int journalMinFiles = DEFAULT_JOURNAL_MIN_FILES;
   
   protected int journalMaxAIO = DEFAULT_JOURNAL_MAX_AIO;
   
   protected List<String> interceptorClassNames = new ArrayList<String>();

   protected Set<AcceptorInfo> acceptorInfos = new HashSet<AcceptorInfo>();
         
   public boolean isClustered()
   {
      return clustered;
   }
   
   public void setClustered(boolean clustered)
   {
      this.clustered = clustered;
   }
   
   public boolean isBackup()
   {
      return backup;
   }
   
   public void setBackup(boolean backup)
   {
      this.backup = backup;
   }
   
   public int getScheduledThreadPoolMaxSize()
   {
   	return scheduledThreadPoolMaxSize;
   }
   
   public void setScheduledThreadPoolMaxSize(int maxSize)
   {
      this.scheduledThreadPoolMaxSize = maxSize;
   }
   
   public long getSecurityInvalidationInterval()
   {
   	return this.securityInvalidationInterval;
   }
   
   public void setSecurityInvalidationInterval(long interval)
   {
      this.securityInvalidationInterval = interval;
   }
   
   public boolean isRequireDestinations()
   {
      return requireDestinations;
   }
   
   public void setRequireDestinations(boolean require)
   {
      this.requireDestinations = require;
   }   
   
   public long getCallTimeout()
   {
      return callTimeout;
   }
   
   public void setCallTimeout(long timeout)
   {
      this.callTimeout = timeout;
   }
   
   public int getPacketConfirmationBatchSize()
   {
      return this.packetConfirmationBatchSize;
   }
   
   public void setPacketConfirmationBatchSize(int size)
   {
      this.packetConfirmationBatchSize = size;
   }
   
   public long getConnectionScanPeriod()
   {
      return connectionScanPeriod;
   }
      
   public void setConnectionScanPeriod(long scanPeriod)
   {
      this.connectionScanPeriod = scanPeriod;
   }
      
   public List<String> getInterceptorClassNames()
   {
      return interceptorClassNames;
   }
   
   public void setInterceptorClassNames(List<String> interceptors)
   {
      this.interceptorClassNames = interceptors;
   }
   
   public Set<AcceptorInfo> getAcceptorInfos()
   {
      return this.acceptorInfos;
   }
   
   public void setAcceptorInfos(Set<AcceptorInfo> infos)
   {
      this.acceptorInfos = infos;
   }
  
	public String getBindingsDirectory()
	{
		return bindingsDirectory;
	}
	
	public void setBindingsDirectory(String dir)
   {
      this.bindingsDirectory = dir;
   }

	public String getJournalDirectory()
	{
		return journalDirectory;
	}
	
	public void setJournalDirectory(String dir)
   {
      this.journalDirectory = dir;
   }

	public JournalType getJournalType()
	{
		return journalType;
	}
	
	public void setJournalType(JournalType type)
   {
      this.journalType = type;
   }
	
	public boolean isJournalSyncTransactional()
	{
		return journalSyncTransactional;
	}
	
	public void setJournalSyncTransactional(boolean sync)
   {
      this.journalSyncTransactional = sync;
   }
	
	public boolean isJournalSyncNonTransactional()
   {
      return journalSyncNonTransactional;
   }
	
	public void setJournalSyncNonTransactional(boolean sync)
   {
      this.journalSyncNonTransactional = sync;
   }

	public int getJournalFileSize()
	{
		return journalFileSize;
	}
	
	public void setJournalFileSize(int size)
   {
      this.journalFileSize = size;
   }

	public int getJournalMaxAIO()
	{
	   return journalMaxAIO;
	}
	
	public void setJournalMaxAIO(int maxAIO)
   {
      this.journalMaxAIO = maxAIO;
   }
	
   public int getJournalMinFiles()
	{
		return journalMinFiles;
	}
   
   public void setJournalMinFiles(int files)
   {
      this.journalMinFiles = files;
   }

	public boolean isCreateBindingsDir()
	{
		return createBindingsDir;
	}

	public void setCreateBindingsDir(boolean create)
	{
	   this.createBindingsDir = create;
	}

	public boolean isCreateJournalDir()
	{
		return createJournalDir;
	}
	
	public void setCreateJournalDir(boolean create)
   {
      this.createJournalDir = create;
   }

	public boolean isSecurityEnabled()
	{
	   return securityEnabled;
	}
	
	public void setSecurityEnabled(boolean enabled)
   {
      this.securityEnabled = enabled;
   }

	public boolean isJMXManagementEnabled()
	{
	   return jmxManagementEnabled ;
	}
	
	public void setJMXManagementEnabled(boolean enabled)
	{
	   this.jmxManagementEnabled = enabled;
	}
	
   public boolean equals(Object other)
   {
      if (this == other)
      {
         return true;
      }
      
      if (other instanceof Configuration == false)
      {
         return false;
      }
      
      Configuration cother = (Configuration)other;
      
      return cother.isClustered() == this.isClustered() &&
             cother.isCreateBindingsDir() == this.isCreateBindingsDir() &&
             cother.isCreateJournalDir() == this.isCreateJournalDir() &&
             cother.isJournalSyncNonTransactional() == this.isJournalSyncNonTransactional() &&
             cother.isJournalSyncTransactional() == this.isJournalSyncTransactional() &&
             cother.isRequireDestinations() == this.isRequireDestinations() &&
             cother.isSecurityEnabled() == this.isSecurityEnabled() &&
             cother.getBindingsDirectory().equals(this.getBindingsDirectory()) &&
             cother.getJournalDirectory().equals(this.getJournalDirectory()) &&
             cother.getJournalFileSize() == this.getJournalFileSize() &&
             cother.getJournalMaxAIO() == this.getJournalMaxAIO() &&
             cother.getJournalMinFiles() == this.getJournalMinFiles() &&
             cother.getJournalType() == this.getJournalType() &&
             cother.getScheduledThreadPoolMaxSize() == this.getScheduledThreadPoolMaxSize() &&
             cother.getSecurityInvalidationInterval() == this.getSecurityInvalidationInterval();
   }

}
 
