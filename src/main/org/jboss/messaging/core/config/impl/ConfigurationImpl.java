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

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.ConnectionParams;

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
   public static final int DEFAULT_KEEP_ALIVE_INTERVAL = 10; // in seconds
   public static final int DEFAULT_KEEP_ALIVE_TIMEOUT = 5; // in seconds
   public static final int DEFAULT_REQRES_TIMEOUT = 5; // in seconds
   public static final boolean DEFAULT_INVM_DISABLED = false;
   public static final boolean DEFAULT_SSL_ENABLED = false;

   private PropertyChangeSupport propertyChangeSupport;
   
   protected int messagingServerID = 0;
   
   protected String securityDomain;
   
   protected List<String> defaultInterceptors = new ArrayList<String>();

   protected long messageCounterSamplePeriod = (long) 10000;// Default is 1 minute

   protected int defaultMessageCounterHistoryDayLimit = 1;

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
   
   protected long journalTaskPeriod;

   // remoting config
   
   protected TransportType transport;
   protected String host;
   protected int port = DEFAULT_REMOTING_PORT;

   protected int timeout = DEFAULT_REQRES_TIMEOUT;
   protected int keepAliveInterval = DEFAULT_KEEP_ALIVE_INTERVAL;
   protected int keepAliveTimeout = DEFAULT_KEEP_ALIVE_TIMEOUT;
   protected boolean invmDisabled = DEFAULT_INVM_DISABLED;
   protected boolean invmDisabledModified = false;
   protected boolean tcpNoDelay;
   protected int tcpReceiveBufferSize = -1;
   protected int tcpSendBufferSize = -1;
   protected boolean sslEnabled = DEFAULT_SSL_ENABLED;
   protected boolean sslEnabledModified = false;
   protected String keyStorePath;
   protected String keyStorePassword;
   protected String trustStorePath;
   protected String trustStorePassword;

   
   public void addPropertyChangeListener(PropertyChangeListener listener)
   {
      if (propertyChangeSupport == null)
      {
         propertyChangeSupport = new PropertyChangeSupport(this);
      }
      propertyChangeSupport.addPropertyChangeListener(listener);
   }

   public int getMessagingServerID()
   {
      return messagingServerID;
   }
   
   public void setMessagingServerID(int id)
   {
   	this.messagingServerID = id;
   }
  
   public String getSecurityDomain()
   {
      return securityDomain;
   }
   
   public List<String> getDefaultInterceptors()
   {
      return defaultInterceptors;
   }

   public long getMessageCounterSamplePeriod()
   {
      return messageCounterSamplePeriod;
   }

   public void setMessageCounterSamplePeriod(long messageCounterSamplePeriod)
   {
      if (messageCounterSamplePeriod < 1000)
      {
         throw new IllegalArgumentException("Cannot set MessageCounterSamplePeriod < 1000 ms");
      }
      
      propertyChangeSupport.firePropertyChange("messageCounterSamplePeriod", this.messageCounterSamplePeriod, messageCounterSamplePeriod);
      
      this.messageCounterSamplePeriod = messageCounterSamplePeriod;
   }

   public Integer getDefaultMessageCounterHistoryDayLimit()
   {
      return defaultMessageCounterHistoryDayLimit;
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
      return new LocationImpl(transport, host, port);
   }
   
   public int getKeepAliveInterval()
   {
      return keepAliveInterval;
   }
   
   public void setKeepAliveInterval(int keepAliveInterval)
   {
      this.keepAliveInterval = keepAliveInterval;
   }

   public int getKeepAliveTimeout()
   {
      return keepAliveTimeout;
   }

   public void setKeepAliveTimeout(int keepAliveTimeout)
   {
      this.keepAliveTimeout = keepAliveTimeout;
   }
   
   public int getTimeout()
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

	public boolean isJournalSync()
	{
		return journalSync;
	}

	public int getJournalFileSize()
	{
		return journalFileSize;
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
      return connectionParams;
   }
}
 
