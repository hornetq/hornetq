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
package org.jboss.messaging.core.server;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.RemotingConfigurationImpl;

/**
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class Configuration implements RemotingConfiguration, Serializable
{
   private static final long serialVersionUID = 4077088945050267843L;

   private static final String REMOTING_DISABLE_INVM_SYSPROP_KEY = "jbm.remoting.disable.invm";

   public static final String REMOTING_ENABLE_SSL_SYSPROP_KEY = "jbm.remoting.enable.ssl";

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

   protected RemotingConfigurationImpl remotingConfig;
   
   protected boolean requireDestinations;
   
   //Persistence config
   
   protected String bindingsDirectory;
   
   protected boolean createBindingsDir;
   
   protected String journalDirectory;
   
   protected boolean createJournalDir;
   
   protected JournalType journalType;
   
   protected boolean journalSync;
   
   protected int journalFileSize;
   
   protected int journalMinFiles;
   
   protected int journalMinAvailableFiles;
   
   protected long journalTaskPeriod;

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
  
   public void setSecurityDomain(String securityDomain) throws Exception
   {
      this.securityDomain = securityDomain;
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

   public void setDefaultMessageCounterHistoryDayLimit(Integer defaultMessageCounterHistoryDayLimit)
   {
      if (defaultMessageCounterHistoryDayLimit < -1)
      {
         defaultMessageCounterHistoryDayLimit = -1;
      }
      
      this.defaultMessageCounterHistoryDayLimit = defaultMessageCounterHistoryDayLimit;
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
   
   public void setScheduledThreadPoolMaxSize(int size)
   {
   	this.scheduledThreadPoolMaxSize = size;
   }

   public void setClustered(Boolean clustered)
   {
      this.clustered = clustered;
   }
   
   public long getSecurityInvalidationInterval()
   {
   	return this.securityInvalidationInterval;
   }
   
   public String getHost()
   {
	   return remotingConfig.getHost();
	}
   
   // FIXME required only for tests
   public void setPort(int port)
   {
      remotingConfig.setPort(port);
   }

   public int getPort()
   {
	   return remotingConfig.getPort();
   }

   public TransportType getTransport() 
   {
	   return remotingConfig.getTransport();
   }

   public boolean isInvmDisabled()
   {
      if (System.getProperty(REMOTING_DISABLE_INVM_SYSPROP_KEY) != null)
      {
         return Boolean.parseBoolean(System.getProperty(REMOTING_DISABLE_INVM_SYSPROP_KEY));
      }
      else 
      {
         return remotingConfig.isInvmDisabled();
      }
   }

   public boolean isSSLEnabled()
   {
      if (System.getProperty(REMOTING_ENABLE_SSL_SYSPROP_KEY) != null)
      {
         return Boolean.parseBoolean(System.getProperty(REMOTING_ENABLE_SSL_SYSPROP_KEY));
      }
      else 
      {
         return remotingConfig.isSSLEnabled();
      }
   }

   public int getKeepAliveInterval()
   {
      return remotingConfig.getKeepAliveInterval();
   }

   public int getKeepAliveTimeout()
   {
      return remotingConfig.getKeepAliveTimeout();
   }

   public String getKeyStorePassword()
   {
      return remotingConfig.getKeyStorePassword();
   }

   public String getKeyStorePath()
   {
      return remotingConfig.getKeyStorePath();
   }

   public int getTimeout()
   {
      return remotingConfig.getTimeout();
   }

   public String getTrustStorePassword()
   {
      return remotingConfig.getTrustStorePassword();
   }

   public String getTrustStorePath()
   {
      return remotingConfig.getTrustStorePath();
   }
   
   public String getURI()
   {
      return remotingConfig.getURI();
   }

	public String getBindingsDirectory()
	{
		return bindingsDirectory;
	}

	public void setBindingsDirectory(String bindingsDirectory)
	{
		this.bindingsDirectory = bindingsDirectory;
	}

	public String getJournalDirectory()
	{
		return journalDirectory;
	}

	public void setJournalDirectory(String journalDirectory)
	{
		this.journalDirectory = journalDirectory;
	}

	public JournalType getJournalType()
	{
		return journalType;
	}

	public void setJournalType(JournalType journalType)
	{
		this.journalType = journalType;
	}

	public boolean isJournalSync()
	{
		return journalSync;
	}

	public void setJournalSync(boolean journalSync)
	{
		this.journalSync = journalSync;
	}

	public int getJournalFileSize()
	{
		return journalFileSize;
	}

	public void setJournalFileSize(int journalFileSize)
	{
		this.journalFileSize = journalFileSize;
	}

	public int getJournalMinFiles()
	{
		return journalMinFiles;
	}

	public void setJournalMinFiles(int journalMinFiles)
	{
		this.journalMinFiles = journalMinFiles;
	}

	public int getJournalMinAvailableFiles()
	{
		return journalMinAvailableFiles;
	}

	public void setJournalMinAvailableFiles(int journalMinAvailableFiles)
	{
		this.journalMinAvailableFiles = journalMinAvailableFiles;
	}

	public long getJournalTaskPeriod()
	{
		return journalTaskPeriod;
	}

	public void setJournalTaskPeriod(long journalTaskPeriod)
	{
		this.journalTaskPeriod = journalTaskPeriod;
	}

	public boolean isCreateBindingsDir()
	{
		return createBindingsDir;
	}

	public void setCreateBindingsDir(boolean createBindingsDir)
	{
		this.createBindingsDir = createBindingsDir;
	}

	public boolean isCreateJournalDir()
	{
		return createJournalDir;
	}

	public void setCreateJournalDir(boolean createJournalDir)
	{
		this.createJournalDir = createJournalDir;
	}

	public boolean isRequireDestinations()
	{
		return requireDestinations;
	}

	public void setRequireDestinations(boolean requireDestinations)
	{
		this.requireDestinations = requireDestinations;
	}


   
//   /**
//    * If the system property <code>jbm.remoting.disable.invm</code> is set, its boolean value is used 
//    * regardless of the value of the property <code>remoting-disable-invm</code> in <code>jbm-configuration.xml</code>
//    */
//   public RemotingConfiguration getRemotingConfiguration() 
//   {
//      RemotingConfigurationImpl configuration = new RemotingConfigurationImpl(remotingTransport, "localhost", remotingBindAddress);
//      
//      configuration.setTimeout(remotingTimeout);
//      
//      if (System.getProperty(REMOTING_DISABLE_INVM_SYSPROP_KEY) != null)
//      {
//         configuration.setInvmDisabled(Boolean.parseBoolean(System.getProperty(REMOTING_DISABLE_INVM_SYSPROP_KEY)));
//      }
//      else 
//      {
//         configuration.setInvmDisabled(remotingDisableInvm);
//      }
//      
//      if (System.getProperty(REMOTING_ENABLE_SSL_SYSPROP_KEY) != null)
//      {
//         configuration.setSSLEnabled(Boolean.parseBoolean(System.getProperty(REMOTING_ENABLE_SSL_SYSPROP_KEY)));
//      }
//      else 
//      {
//         configuration.setSSLEnabled(remotingEnableSSL);
//      }
//      
//      configuration.setKeyStorePath(remotingSSLKeyStorePath);
//      
//      configuration.setKeyStorePassword(remotingSSLKeyStorePassword);
//      
//      configuration.setTrustStorePath(remotingSSLTrustStorePath);
//      
//      configuration.setTrustStorePassword(remotingSSLTrustStorePassword); 
//      
//      return configuration;
//   }
}
 
