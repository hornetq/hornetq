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
package org.jboss.messaging.core;

import static org.jboss.messaging.core.remoting.TransportType.TCP;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.jboss.jms.server.security.Role;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.TransportType;

/**
 * This is the JBM configuration. It is used to configure the MessagingServer.
 * 
 * It does this by parsing the jbm-configuration.xml configuration file.
 * 
 * It also uses PropertyChangeSupport so users of this class can be notified on configuration changes.
 *
 * Derived from old ServerPeer
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 */
public class Configuration implements Serializable
{
   private static final long serialVersionUID = -95502236335483837L;


   private PropertyChangeSupport propertyChangeSupport;
   protected Integer messagingServerID = 0;
   protected String _defaultQueueJNDIContext = "";
   protected String _defaultTopicJNDIContext = "";
   protected String _securityDomain;
   protected HashSet<Role> _securityConfig;
   protected List<String> defaultInterceptors = new ArrayList<String>();

   protected Long _messageCounterSamplePeriod = (long) 10000;// Default is 1 minute

   protected Integer _defaultMessageCounterHistoryDayLimit = 1;

   //Global override for strict behaviour
   protected Boolean _strictTck = false;

   protected String _postOfficeName;

   protected Boolean _clustered = false;

   protected Long _stateTimeout = (long) 5000;

   protected Long _castTimeout = (long) 5000;

   protected String _groupName;

   protected String _controlChannelName;

   protected String _dataChannelName;

   protected String _channelPartitionName;

   protected TransportType _remotingTransport = TCP;

   protected Integer _remotingBindAddress;
   
   protected Integer _remotingTimeout;

   protected Boolean _remotingDisableInvm = false;

   public  void addPropertyChangeListener(
         PropertyChangeListener listener)
   {
      if(propertyChangeSupport == null)
      {
         propertyChangeSupport = new PropertyChangeSupport(this);
      }
      propertyChangeSupport.addPropertyChangeListener(listener);
   }

   public  Integer getMessagingServerID()
   {
      return messagingServerID;
   }

   public  void setMessagingServerID(Integer messagingServerID)
   {
      this.messagingServerID = messagingServerID;
   }

   public  String getDefaultQueueJNDIContext()
   {
      return _defaultQueueJNDIContext;
   }

   public  void setDefaultQueueJNDIContext(String defaultQueueJNDIContext)
   {
      _defaultQueueJNDIContext = defaultQueueJNDIContext;
   }

   public  String getDefaultTopicJNDIContext()
   {
      return _defaultTopicJNDIContext;
   }

   public  void setDefaultTopicJNDIContext(String defaultTopicJNDIContext)
   {
      _defaultTopicJNDIContext = defaultTopicJNDIContext;
   }

   public  void setSecurityDomain(String securityDomain) throws Exception
   {
      _securityDomain = securityDomain;
   }

   public  String getSecurityDomain()
   {
      return _securityDomain;
   }

   public  HashSet<Role> getSecurityConfig()
   {
      return _securityConfig;
   }
   
   public List<String> getDefaultInterceptors()
   {
      return defaultInterceptors;
   }

   public  void setSecurityConfig(HashSet<Role> securityConfig)
   {
      propertyChangeSupport.firePropertyChange("securityConfig", _securityConfig, securityConfig);
      _securityConfig = securityConfig;
   }

   public  long getMessageCounterSamplePeriod()
   {
      return _messageCounterSamplePeriod;
   }

   public  void setMessageCounterSamplePeriod(long messageCounterSamplePeriod)
   {
      if (messageCounterSamplePeriod < 1000)
      {
         throw new IllegalArgumentException("Cannot set MessageCounterSamplePeriod < 1000 ms");
      }
      propertyChangeSupport.firePropertyChange("messageCounterSamplePeriod", _messageCounterSamplePeriod, messageCounterSamplePeriod);
      _messageCounterSamplePeriod = messageCounterSamplePeriod;
   }

   public  Integer getDefaultMessageCounterHistoryDayLimit()
   {
      return _defaultMessageCounterHistoryDayLimit;
   }

   public  void setDefaultMessageCounterHistoryDayLimit(Integer defaultMessageCounterHistoryDayLimit)
   {
      if (defaultMessageCounterHistoryDayLimit < -1)
      {
         defaultMessageCounterHistoryDayLimit = -1;
      }
      _defaultMessageCounterHistoryDayLimit = defaultMessageCounterHistoryDayLimit;
   }

   public  Boolean isStrictTck()
   {
      return _strictTck || "true".equalsIgnoreCase(System.getProperty("jboss.messaging.stricttck"));
   }

   public  void setStrictTck(Boolean strictTck)
   {
      _strictTck = strictTck || "true".equalsIgnoreCase(System.getProperty("jboss.messaging.stricttck"));
   }

   public  String getPostOfficeName()
   {
      return _postOfficeName;
   }

   public  void setPostOfficeName(String postOfficeName)
   {
      _postOfficeName = postOfficeName;
   }

   public  Boolean isClustered()
   {
      return _clustered;
   }

   public  void setClustered(Boolean clustered)
   {
      _clustered = clustered;
   }

   public  Long getStateTimeout()
   {
      return _stateTimeout;
   }

   public  void setStateTimeout(Long stateTimeout)
   {
      _stateTimeout = stateTimeout;
   }

   public  Long getCastTimeout()
   {
      return _castTimeout;
   }

   public  void setCastTimeout(Long castTimeout)
   {
      _castTimeout = castTimeout;
   }

   public  String getGroupName()
   {
      return _groupName;
   }

   public  void setGroupName(String groupName)
   {
      _groupName = groupName;
   }


   public  String getControlChannelName()
   {
      return _controlChannelName;
   }

   public  void setControlChannelName(String controlChannelName)
   {
      _controlChannelName = controlChannelName;
   }


   public  String getDataChannelName()
   {
      return _dataChannelName;
   }

   public  void setDataChannelName(String dataChannelName)
   {
      _dataChannelName = dataChannelName;
   }


   public  String getChannelPartitionName()
   {
      return _channelPartitionName;
   }

   public  void setChannelPartitionName(String channelPartitionName)
   {
      _channelPartitionName = channelPartitionName;
   }

   public Integer getRemotingBindAddress()
   {
      return _remotingBindAddress;
   }

   public void setRemotingBindAddress(Integer remotingBindAddress)
   {
      this._remotingBindAddress = remotingBindAddress;
   }

   public RemotingConfiguration getRemotingConfiguration() 
   {
      RemotingConfiguration configuration = new RemotingConfiguration(_remotingTransport, "localhost", _remotingBindAddress);
      configuration.setTimeout(_remotingTimeout);
      configuration.setInvmDisabled(_remotingDisableInvm);
      return configuration;
   }

}
 
