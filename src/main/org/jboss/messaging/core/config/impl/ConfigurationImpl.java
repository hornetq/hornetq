/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

import java.io.Serializable;
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
public class ConfigurationImpl implements Configuration, Serializable
{
   private static final long serialVersionUID = 4077088945050267843L;

   public static final String REMOTING_DISABLE_INVM_SYSPROP_KEY = "jbm.remoting.disable.invm";

   public static final String REMOTING_ENABLE_SSL_SYSPROP_KEY = "jbm.remoting.enable.ssl";

   public static final int DEFAULT_REMOTING_PORT = 5400;
   public static final boolean DEFAULT_INVM_DISABLED = false;
   public static final boolean DEFAULT_SSL_ENABLED = false;
   public static final int DEFAULT_MAX_AIO = 3000;
   public static final long DEFAULT_AIO_TIMEOUT = 90000; // in ms
   
   protected List<String> defaultInterceptors = new ArrayList<String>();

   protected boolean strictTck = false;

   protected boolean clustered = false;
   
   protected int scheduledThreadPoolMaxSize = 30;
   
   protected long securityInvalidationInterval = 10000;

   protected boolean requireDestinations;
   
   //Persistence config
   
   protected String bindingsDirectory;
   
   protected boolean createBindingsDir;
   
   protected String journalDirectory;
   
   protected boolean createJournalDir;
   
   public JournalType journalType;
   
   protected boolean journalSync;
   
   protected int journalFileSize;
   
   protected int journalMinFiles;
   
   protected int journalMaxAIO;
   
   protected long journalAIOTimeout;
   
   protected long journalTaskPeriod;
   
   protected boolean securityEnabled = true;

   // remoting config
   
   protected TransportType transport;
   protected String host;
   protected int port = DEFAULT_REMOTING_PORT;
   protected int serverID = 0;
   
   protected long timeout = ConnectionParams.DEFAULT_REQRES_TIMEOUT;
   protected long keepAliveInterval = ConnectionParams.DEFAULT_KEEP_ALIVE_INTERVAL;
   protected long keepAliveTimeout = ConnectionParams.DEFAULT_KEEP_ALIVE_TIMEOUT;
   protected boolean invmDisabled = DEFAULT_INVM_DISABLED;
   protected boolean invmDisabledModified = false;
   protected boolean tcpNoDelay;
   protected long writeQueueBlockTimeout = 5000;
   protected long writeQueueMinBytes = 65536;
   protected long writeQueueMaxBytes = 1048576;
   
   protected int tcpReceiveBufferSize = -1;
   protected int tcpSendBufferSize = -1;
   protected boolean sslEnabled = DEFAULT_SSL_ENABLED;
   protected boolean sslEnabledModified = false;
   protected String keyStorePath;
   protected String keyStorePassword;
   protected String trustStorePath;
   protected String trustStorePassword;

   public List<String> getDefaultInterceptors()
   {
      return defaultInterceptors;
   }

   public Boolean isStrictTck()
   {
      return strictTck || "true".equalsIgnoreCase(System.getProperty("jboss.messaging.stricttck"));
   }

   public void setStrictTck(Boolean strictTck)
   {
      strictTck = strictTck || "true".equalsIgnoreCase(System.getProperty("jboss.messaging.stricttck"));
   }
  
   public Boolean isClustered()
   {
      return clustered;
   }
   
   public Integer getScheduledThreadPoolMaxSize()
   {
   	return scheduledThreadPoolMaxSize;
   }
      
   public long getSecurityInvalidationInterval()
   {
   	return this.securityInvalidationInterval;
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
      assert host != null;
      
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
         return new LocationImpl(serverID);
      else
         return new LocationImpl(transport, host, port);
   }
   
   public int getServerID()
   {
      return serverID;
   }
   
   public void setServerID(int serverID)
   {
      this.serverID = serverID;
   }
   
   public long getKeepAliveInterval()
   {
      return keepAliveInterval;
   }
   
   public void setKeepAliveInterval(long keepAliveInterval)
   {
      this.keepAliveInterval = keepAliveInterval;
   }

   public long getKeepAliveTimeout()
   {
      return keepAliveTimeout;
   }

   public void setKeepAliveTimeout(long keepAliveTimeout)
   {
      this.keepAliveTimeout = keepAliveTimeout;
   }
   
   public long getTimeout()
   {
      return timeout;
   }

   public String getKeyStorePath()
   {
      return keyStorePath;
   }

   public void setKeyStorePath(String keyStorePath)
   {
      this.keyStorePath = keyStorePath;
   }
   
   public String getKeyStorePassword()
   {
      return keyStorePassword;
   }
   
   public void setKeyStorePassword(String keyStorePassword)
   {
      this.keyStorePassword = keyStorePassword;
   }

   public String getTrustStorePath()
   {
      return trustStorePath;
   }

   public void setTrustStorePath(String trustStorePath)
   {
      this.trustStorePath = trustStorePath;
   }
   
   public String getTrustStorePassword()
   {
      return trustStorePassword;
   }
   
   public void setTrustStorePassword(String trustStorePassword)
   {
      this.trustStorePassword = trustStorePassword;
   }
   
   public boolean isInvmDisabled()
   {
       if (System.getProperty(REMOTING_DISABLE_INVM_SYSPROP_KEY) != null && !invmDisabledModified)
      {
         return Boolean.parseBoolean(System.getProperty(REMOTING_DISABLE_INVM_SYSPROP_KEY));
      }
      else 
      {
         return invmDisabled;
      }
   }
   
   public void setInvmDisabled(boolean invmDisabled)
   {
      this.invmDisabled = invmDisabled;
      this.invmDisabledModified = true;
   }
   
   public void setSecurityEnabled(final boolean enabled)
   {
      this.securityEnabled = enabled;
   }
   
   public boolean isSSLEnabled()
   {
      if (System.getProperty(REMOTING_ENABLE_SSL_SYSPROP_KEY) != null && !sslEnabledModified)
      {
         return Boolean.parseBoolean(System.getProperty(REMOTING_ENABLE_SSL_SYSPROP_KEY));
      }
      else 
      {
         return sslEnabled;
      }
   }
   
   public void setSSLEnabled(boolean sslEnabled)
   {
      this.sslEnabled = sslEnabled;
      this.sslEnabledModified = true;
   }

   public boolean isTcpNoDelay()
   {
      return this.tcpNoDelay;
   }

   public void setTcpNoDelay(boolean tcpNoDelay)
   {
      this.tcpNoDelay = tcpNoDelay;
   }

   public int getTcpReceiveBufferSize()
   {
      return this.tcpReceiveBufferSize;
   }
   
   public void setTcpReceiveBufferSize(int size)
   {
      this.tcpReceiveBufferSize = size;
   }
   
   public int getTcpSendBufferSize()
   {
      return this.tcpSendBufferSize;
   }
   
   public void setTcpSendBufferSize(int size)
   {
      this.tcpSendBufferSize = size;
   }
     
   public long getWriteQueueBlockTimeout()
   {
      return writeQueueBlockTimeout;
   }

   public long getWriteQueueMaxBytes()
   {
      return writeQueueMaxBytes;
   }

   public long getWriteQueueMinBytes()
   {
      return writeQueueMinBytes;
   }
   
   public void setWriteQueueBlockTimeout(final long timeout)
   {
      this.writeQueueBlockTimeout = timeout;
   }

   public void setWriteQueueMaxBytes(final long bytes)
   {
      this.writeQueueMaxBytes = bytes;
   }

   public void setWriteQueueMinBytes(final long bytes)
   {
      this.writeQueueMinBytes = bytes;
   }

   public String getURI()
   {
      StringBuffer buff = new StringBuffer();
      buff.append(transport + "://" + host + ":" + port);
      buff.append("?").append("timeout=").append(timeout);
      buff.append("&").append("keepAliveInterval=").append(keepAliveInterval);
      buff.append("&").append("keepAliveTimeout=").append(keepAliveTimeout);
      buff.append("&").append("invmDisabled=").append(invmDisabled);
      buff.append("&").append("tcpNoDelay=").append(tcpNoDelay);
      buff.append("&").append("tcpReceiveBufferSize=").append(tcpReceiveBufferSize);
      buff.append("&").append("tcpSendBufferSize=").append(tcpSendBufferSize);
      buff.append("&").append("sslEnabled=").append(sslEnabled);
      buff.append("&").append("keyStorePath=").append(keyStorePath);
      buff.append("&").append("trustStorePath=").append(trustStorePath);
      return buff.toString();
   }
   
	public String getBindingsDirectory()
	{
		return bindingsDirectory;
	}

	public String getJournalDirectory()
	{
		return journalDirectory;
	}

	public JournalType getJournalType()
	{
		return journalType;
	}
	
	public void setJournalType(JournalType type)
	{
	   this.journalType = type;
	}

	public boolean isJournalSync()
	{
		return journalSync;
	}

	public int getJournalFileSize()
	{
		return journalFileSize;
	}

	public int getJournalMaxAIO()
	{
	   return journalMaxAIO;
	}
	
	public void setJournalMaxAIO(int max)
	{
	   this.journalMaxAIO = max;
	}
	
	public long getJournalAIOTimeout()
   {
      return journalAIOTimeout;
   }

   public int getJournalMinFiles()
	{
		return journalMinFiles;
	}

	public long getJournalTaskPeriod()
	{
		return journalTaskPeriod;
	}

	public boolean isCreateBindingsDir()
	{
		return createBindingsDir;
	}

	public boolean isCreateJournalDir()
	{
		return createJournalDir;
	}

	public boolean isRequireDestinations()
	{
		return requireDestinations;
	}

	public boolean isSecurityEnabled()
	{
	   return securityEnabled;
	}

   public ConnectionParams getConnectionParams()
   {
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setInvmDisabled(invmDisabled);
      connectionParams.setInvmDisabledModified(invmDisabledModified);
      connectionParams.setKeepAliveInterval(keepAliveInterval);
      connectionParams.setKeepAliveTimeout(keepAliveTimeout);
      connectionParams.setSSLEnabled(sslEnabled);
      connectionParams.setSSLEnabledModified(sslEnabledModified);
      connectionParams.setTcpNoDelay(tcpNoDelay);
      connectionParams.setTcpReceiveBufferSize(tcpReceiveBufferSize);
      connectionParams.setTcpSendBufferSize(tcpSendBufferSize);
      connectionParams.setTimeout(timeout);
      connectionParams.setWriteQueueBlockTimeout(writeQueueBlockTimeout);
      connectionParams.setWriteQueueMinBytes(writeQueueMinBytes);
      connectionParams.setWriteQueueMaxBytes(writeQueueMaxBytes);
      return connectionParams;
   }
  
}
 
