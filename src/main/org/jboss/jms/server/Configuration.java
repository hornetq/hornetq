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
package org.jboss.jms.server;

import org.jboss.jms.server.security.Role;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.Serializable;
import java.net.URL;
import java.util.HashSet;

/**
 * This is the JBM configuration. It is used to configure the ServerPeer.
 * It does this by parsing the jbm-configuration.xml configuration file. It also uses PropertyChangeSupport so users of
 * this class can be notified on configuration changes.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class Configuration implements Serializable
{
   private static final String READ_ATTR = "read";
   private static final String WRITE_ATTR = "write";
   private static final String CREATE_ATTR = "create";
   private static final String NAME_ATTR = "name";

   private  PropertyChangeSupport propertyChangeSupport;
   private  Integer _serverPeerID = -1;
   private  String _defaultQueueJNDIContext = "";
   private  String _defaultTopicJNDIContext = "";
   private  String _securityDomain;
   private  HashSet<Role> _securityConfig;
   private  String _defaultDLQ;
   // The default maximum number of delivery attempts before sending to DLQ - can be overridden on
   // the destination
   private  Integer _defaultMaxDeliveryAttempts = 10;
   protected  String _defaultExpiryQueue;

   private  Long _defaultRedeliveryDelay = (long) 0;

   private  Long _messageCounterSamplePeriod = (long) 10000;// Default is 1 minute
   private  Long _failoverStartTimeout = (long) (60 * 1000);
   // Default is 5 minutes
   private  Long _failoverCompleteTimeout = (long) (5 * 60 * 1000);

   private  Integer _defaultMessageCounterHistoryDayLimit = 1;

   private  String _clusterPullConnectionFactoryName;


   private  Boolean _useXAForMessagePull = false;

   private  Boolean _defaultPreserveOrdering = false;

   private  Long _recoverDeliveriesTimeout = (long) (5 * 60 * 1000);


   private  String _suckerPassword;

   //Global override for strict behaviour
   private  Boolean _strictTck = false;

   //From a system property - this overrides
   private  Boolean _strictTckProperty = false;

   private  String _postOfficeName;

   private  Boolean _clustered = false;

   private  Long _stateTimeout = (long) 5000;

   private  Long _castTimeout = (long) 5000;

   private  String _groupName;

   private  String _controlChannelName;

   private  String _dataChannelName;

   private  String _channelPartitionName;

   private  Integer _maxConcurrentReplications = 25;

   private  Boolean _useJGroupsWorkaround = false;

   private Integer _remotingBindAddress;

   private String _remotingTimeout;

   //default confog file location
   private String configurationUrl = "jbm-configuration.xml";

   public void start() throws Exception
   {
      propertyChangeSupport = new PropertyChangeSupport(this);

      _strictTckProperty = "true".equalsIgnoreCase(System.getProperty("jboss.messaging.stricttck"));
      _useJGroupsWorkaround = "true".equals(System.getProperty("jboss.messaging.usejgroupsworkaround"));

      URL url = getClass().getClassLoader().getResource(configurationUrl);
      Element e = XMLUtil.urlToElement(url);
      _serverPeerID = getInteger(e, "server-peer-id", _serverPeerID);
      _defaultQueueJNDIContext = getString(e, "default-queue-jndi-context", _defaultQueueJNDIContext);
      _defaultTopicJNDIContext = getString(e, "default-topic-jndi-context", _defaultTopicJNDIContext);
      _securityDomain = getString(e, "security-domain", _securityDomain);
      _defaultDLQ = getString(e, "default-dlq", _defaultDLQ);
      _defaultMaxDeliveryAttempts = getInteger(e, "default-max-delivery-attempts", _defaultMaxDeliveryAttempts);
      _defaultExpiryQueue = getString(e, "default-expiry-queue", _defaultExpiryQueue);
      _defaultRedeliveryDelay = getLong(e, "default-redelivery-delay", _defaultRedeliveryDelay);
      _messageCounterSamplePeriod = getLong(e, "message-counter-sample-period", _messageCounterSamplePeriod);
      _failoverStartTimeout = getLong(e, "failover-start-timeout", _failoverStartTimeout);
      _failoverCompleteTimeout = getLong(e, "failover-complete-timeout", _failoverCompleteTimeout);
      _defaultMessageCounterHistoryDayLimit = getInteger(e, "default-message-counter-history-day-limit", _defaultMessageCounterHistoryDayLimit);
      _clusterPullConnectionFactoryName = getString(e, "cluster-pull-connection-factory-name", _clusterPullConnectionFactoryName);
      _useXAForMessagePull = getBoolean(e, "use-xa-for-message-pull", _useXAForMessagePull);
      _defaultPreserveOrdering = getBoolean(e, "default-preserve-ordering", _defaultPreserveOrdering);
      _recoverDeliveriesTimeout = getLong(e, "recover-deliveries-timeout", _recoverDeliveriesTimeout);
      _suckerPassword = getString(e, "sucker-password", _suckerPassword);
      _strictTck = getBoolean(e, "strict-tck", _strictTck);
      _postOfficeName = getString(e, "post-office-name", _postOfficeName);
      _clustered = getBoolean(e, "clustered", _clustered);
      _stateTimeout = getLong(e, "state-timeout", _stateTimeout);
      _castTimeout = getLong(e, "cast-timeout", _castTimeout);
      _groupName = getString(e, "group-name", _groupName);
      _controlChannelName = getString(e, "control-channel-name", _controlChannelName);
      _dataChannelName = getString(e, "data-channel-name", _dataChannelName);
      _channelPartitionName = getString(e, "channel-partition-name", _channelPartitionName);
      _maxConcurrentReplications = getInteger(e, "max-concurrent-replications", _maxConcurrentReplications);
      _remotingBindAddress = getInteger(e, "remoting-bind-address", _remotingBindAddress);
      _remotingTimeout = getString(e, "remoting-timeout", _remotingTimeout);
      NodeList security = e.getElementsByTagName("default-security-config");
      if (security.getLength() > 0)
      {
         HashSet<Role> securityConfig;
         securityConfig = new HashSet<Role>();
         NodeList roles = security.item(0).getChildNodes();
         for (int k = 0; k < roles.getLength(); k++)
         {
            if ("role".equalsIgnoreCase(roles.item(k).getNodeName()))
            {
               Boolean read = roles.item(k).getAttributes().getNamedItem(READ_ATTR) != null && Boolean.valueOf(roles.item(k).getAttributes().getNamedItem(READ_ATTR).getNodeValue());
               Boolean write = roles.item(k).getAttributes().getNamedItem(WRITE_ATTR) != null && Boolean.valueOf(roles.item(k).getAttributes().getNamedItem(WRITE_ATTR).getNodeValue());
               Boolean create = roles.item(k).getAttributes().getNamedItem(CREATE_ATTR) != null && Boolean.valueOf(roles.item(k).getAttributes().getNamedItem(CREATE_ATTR).getNodeValue());
               Role role = new Role(roles.item(k).getAttributes().getNamedItem(NAME_ATTR).getNodeValue(),
                       read,
                       write,
                       create);
               securityConfig.add(role);
            }
         }
         _securityConfig = securityConfig;
      }

   }

   private  Boolean getBoolean(Element e, String name, Boolean def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return Boolean.valueOf(nl.item(0).getTextContent().trim());
      }
      return def;
   }


   private  Integer getInteger(Element e, String name, Integer def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return Integer.valueOf(nl.item(0).getTextContent().trim());
      }
      return def;
   }

   private  Long getLong(Element e, String name, Long def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return Long.valueOf(nl.item(0).getTextContent().trim());
      }
      return def;
   }

   private  String getString(Element e, String name, String def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return nl.item(0).getTextContent().trim();
      }
      return def;
   }

   public  void addPropertyChangeListener(
           PropertyChangeListener listener)
   {
      propertyChangeSupport.addPropertyChangeListener(listener);
   }

   public  Integer getServerPeerID()
   {
      return _serverPeerID;
   }

   public  void setServerPeerID(Integer serverPeerID)
   {
      _serverPeerID = serverPeerID;
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

   public  void setSecurityConfig(HashSet<Role> securityConfig)
   {
      propertyChangeSupport.firePropertyChange("securityConfig", _securityConfig, securityConfig);
      _securityConfig = securityConfig;
   }


   public  String getDefaultDLQ()
   {
      return _defaultDLQ;
   }

   public  void setDefaultDLQ(String defaultDLQ)
   {
      _defaultDLQ = defaultDLQ;
   }


   public  Integer getDefaultMaxDeliveryAttempts()
   {
      return _defaultMaxDeliveryAttempts;
   }

   public  void setDefaultMaxDeliveryAttempts(Integer defaultMaxDeliveryAttempts)
   {
      _defaultMaxDeliveryAttempts = defaultMaxDeliveryAttempts;
   }


   public  String getDefaultExpiryQueue()
   {
      return _defaultExpiryQueue;
   }

   public  void setDefaultExpiryQueue(String defaultExpiryQueue)
   {
      _defaultExpiryQueue = defaultExpiryQueue;
   }


   public  long getDefaultRedeliveryDelay()
   {
      return _defaultRedeliveryDelay;
   }

   public  void setDefaultRedeliveryDelay(long defaultRedeliveryDelay)
   {
      _defaultRedeliveryDelay = defaultRedeliveryDelay;
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


   public  Long getFailoverStartTimeout()
   {
      return _failoverStartTimeout;
   }

   public  void setFailoverStartTimeout(Long failoverStartTimeout)
   {
      _failoverStartTimeout = failoverStartTimeout;
   }


   public  Long getFailoverCompleteTimeout()
   {
      return _failoverCompleteTimeout;
   }

   public  void setFailoverCompleteTimeout(Long failoverCompleteTimeout)
   {
      _failoverCompleteTimeout = failoverCompleteTimeout;
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


   public  String getClusterPullConnectionFactoryName()
   {
      return _clusterPullConnectionFactoryName;
   }

   public  void setClusterPullConnectionFactoryName(String clusterPullConnectionFactoryName)
   {
      _clusterPullConnectionFactoryName = clusterPullConnectionFactoryName;
   }


   public  Boolean isUseXAForMessagePull()
   {
      return _useXAForMessagePull;
   }

   public  void setUseXAForMessagePull(Boolean useXAForMessagePull)
   {
      _useXAForMessagePull = useXAForMessagePull;
   }


   public  Boolean isDefaultPreserveOrdering()
   {
      return _defaultPreserveOrdering;
   }

   public  void setDefaultPreserveOrdering(Boolean defaultPreserveOrdering)
   {
      _defaultPreserveOrdering = defaultPreserveOrdering;
   }


   public  Long getRecoverDeliveriesTimeout()
   {
      return _recoverDeliveriesTimeout;
   }

   public  void setRecoverDeliveriesTimeout(Long recoverDeliveriesTimeout)
   {
      _recoverDeliveriesTimeout = recoverDeliveriesTimeout;
   }


   public  String getSuckerPassword()
   {
      return _suckerPassword;
   }

   public  void setSuckerPassword(String suckerPassword)
   {
      _suckerPassword = suckerPassword;
   }


   public  Boolean isStrictTck()
   {
      return _strictTck || _strictTckProperty;
   }

   public  void setStrictTck(Boolean strictTck)
   {
      _strictTck = strictTck || _strictTckProperty;
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


   public  Integer getMaxConcurrentReplications()
   {
      return _maxConcurrentReplications;
   }

   public  void setMaxConcurrentReplications(Integer maxConcurrentReplications)
   {
      _maxConcurrentReplications = maxConcurrentReplications;
   }


   public  Boolean isUseJGroupsWorkaround()
   {
      return _useJGroupsWorkaround;
   }

   public  void setUseJGroupsWorkaround(Boolean useJGroupsWorkaround)
   {
      _useJGroupsWorkaround = useJGroupsWorkaround;
   }

   public Integer getRemotingBindAddress()
   {
      return _remotingBindAddress;
   }
   
   public void setRemotingBindAddress(Integer remotingBindAddress)
   {
      this._remotingBindAddress = remotingBindAddress;
   }

   public String getRemotingTimeout()
   {
      return _remotingTimeout;
   }
   
   public String getConfigurationUrl()
   {
      return configurationUrl;
   }

   public void setConfigurationUrl(String configurationUrl)
   {
      this.configurationUrl = configurationUrl;
   }
}
