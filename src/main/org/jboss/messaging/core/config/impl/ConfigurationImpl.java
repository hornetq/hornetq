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
import java.util.List;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.JournalType;

/**
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConfigurationImpl implements Configuration
{
   // Constants ------------------------------------------------------------------------------
   
   public static final String ENABLE_SSL_PROPERTY_NAME = "jbm.remoting.ssl.enable";

   public static final boolean DEFAULT_CLUSTERED = false;
   
   public static final int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 30;
   
   public static final int DEFAULT_THREAD_POOL_MAX_SIZE = 30;
   
   public static final String DEFAULT_HOST = "localhost";
   
   public static final TransportType DEFAULT_TRANSPORT = TransportType.TCP;
   
   public static final int DEFAULT_PORT = 5400;
   
   public static final long DEFAULT_SECURITY_INVALIDATION_INTERVAL = 10000;
   
   public static final boolean DEFAULT_REQUIRE_DESTINATIONS = false;
   
   public static final boolean DEFAULT_SECURITY_ENABLED = true;
   
   public static final boolean DEFAULT_SSL_ENABLED = false;
   
   public static final String DEFAULT_KEYSTORE_PATH = "messaging.keystore";
   
   public static final String DEFAULT_KEYSTORE_PASSWORD = "secureexample";
   
   public static final String DEFAULT_TRUSTSTORE_PATH = "messaging.truststore";
   
   public static final String DEFAULT_TRUSTSTORE_PASSWORD = "secureexample";
      
   public static final String DEFAULT_BINDINGS_DIRECTORY = "data/bindings";
   
   public static final boolean DEFAULT_CREATE_BINDINGS_DIR = true;
   
   public static final String DEFAULT_JOURNAL_DIR = "data/journal";
   
   public static final boolean DEFAULT_CREATE_JOURNAL_DIR = true;
   
   public static final JournalType DEFAULT_JOURNAL_TYPE = JournalType.ASYNCIO;
   
   public static final boolean DEFAULT_JOURNAL_SYNC_TRANSACTIONAL = true;
   
   public static final boolean DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL = false;
   
   public static final int DEFAULT_JOURNAL_FILE_SIZE = 10485760;
   
   public static final int DEFAULT_JOURNAL_MIN_FILES = 10;
   
   public static final int DEFAULT_MAX_AIO = 5000;
   
   public static final long DEFAULT_AIO_TIMEOUT = 60000; // in ms
   
   public static final long DEFAULT_JOURNAL_TASK_PERIOD = 5000;
   
   
   
   private static final long serialVersionUID = 4077088945050267843L;

   
   // Attributes -----------------------------------------------------------------------------
   
   protected List<String> interceptorClassNames = new ArrayList<String>();

   protected boolean clustered = DEFAULT_CLUSTERED;
   
   protected int scheduledThreadPoolMaxSize = DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
   
   protected int threadPoolMaxSize = DEFAULT_THREAD_POOL_MAX_SIZE;
   
   protected long securityInvalidationInterval = DEFAULT_SECURITY_INVALIDATION_INTERVAL;

   protected boolean requireDestinations = DEFAULT_REQUIRE_DESTINATIONS;
   
   protected boolean securityEnabled = DEFAULT_SECURITY_ENABLED;

   
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
   
   protected int journalMaxAIO = DEFAULT_MAX_AIO;
   
   protected long journalAIOTimeout = DEFAULT_AIO_TIMEOUT;
   
   protected long journalTaskPeriod = DEFAULT_JOURNAL_TASK_PERIOD;
   
   // remoting config
   
   //TODO  - do we really need this sever id??? I don't see why
   protected int serverID = 0;
         
   protected TransportType transport = DEFAULT_TRANSPORT;
   
   protected String host = DEFAULT_HOST;
   
   protected int port = DEFAULT_PORT;
   
   protected final ConnectionParams defaultConnectionParams = new ConnectionParamsImpl();
   
   protected boolean sslEnabled = DEFAULT_SSL_ENABLED;
      
   protected String keyStorePath = DEFAULT_KEYSTORE_PATH;
   
   protected String keyStorePassword = DEFAULT_KEYSTORE_PASSWORD;
   
   protected String trustStorePath = DEFAULT_TRUSTSTORE_PATH;
   
   protected String trustStorePassword = DEFAULT_TRUSTSTORE_PASSWORD;
   
   
   public List<String> getInterceptorClassNames()
   {
      return interceptorClassNames;
   }

   public boolean isClustered()
   {
      return clustered;
   }
   
   public void setClustered(boolean clustered)
   {
      this.clustered = clustered;
   }
   
   public int getScheduledThreadPoolMaxSize()
   {
   	return scheduledThreadPoolMaxSize;
   }
   
   public void setScheduledThreadPoolMaxSize(int maxSize)
   {
      this.scheduledThreadPoolMaxSize = maxSize;
   }
   
   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }
   
   public void setThreadPoolMaxSize(int maxSize)
   {
      this.threadPoolMaxSize = maxSize;
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
   
   public int getServerID()
   {
      return serverID;
   }

   public void setServerID(int id)
   {
      this.serverID = id;
   }
   
   public TransportType getTransport()
   {
      return transport;
   }
   
   public void setTransport(TransportType transport)
   {
      this.transport = transport;
   }

   public String getHost()
   {
      return host;
   }
   
   public void setHost(String host)
   {
      this.host = host;
   }
   
   public int getPort()
   {
      return port;
   }
   
   public void setPort(int port)
   {
      this.port = port;
   }
   
   public Location getLocation()
   {
      if (transport == TransportType.INVM)
      {
         return new LocationImpl(serverID);
      }
      else
      {
         return new LocationImpl(transport, host, port);
      }
   }
   
   public String getKeyStorePath()
   {
      return keyStorePath;
   }
   
   public void setKeyStorePath(String path)
   {
      this.keyStorePath = path;
   }

   public String getKeyStorePassword()
   {
      return keyStorePassword;
   }
   
   public void setKeyStorePassword(String password)
   {
      this.keyStorePassword = password;
   }

   public String getTrustStorePath()
   {
      return trustStorePath;
   }
   
   public void setTrustStorePath(String path)
   {
      this.trustStorePath = path;
   }

   public String getTrustStorePassword()
   {
      return trustStorePassword;
   }
   
   public void setTrustStorePassword(String password)
   {
      this.trustStorePassword = password;
   }

   public boolean isSSLEnabled()
   {
      if (System.getProperty(ENABLE_SSL_PROPERTY_NAME) != null)
      {
         return Boolean.parseBoolean(System.getProperty(ENABLE_SSL_PROPERTY_NAME));
      }
      else 
      {
         return sslEnabled;
      }
   }
   
   public void setSSLEnabled(boolean enabled)
   {
      this.sslEnabled = enabled;
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
	
	public long getJournalAIOTimeout()
   {
      return journalAIOTimeout;
   }
	
	public void setJournalAIOTimeout(long timeout)
   {
      this.journalAIOTimeout = timeout;
   }

   public int getJournalMinFiles()
	{
		return journalMinFiles;
	}
   
   public void setJournalMinFiles(int files)
   {
      this.journalMinFiles = files;
   }

	public long getJournalTaskPeriod()
	{
		return journalTaskPeriod;
	}
	
	public void setJournalTaskPeriod(long period)
   {
      this.journalTaskPeriod = period;
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

   public ConnectionParams getConnectionParams()
   {
      return this.defaultConnectionParams;
   }

}
 
