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

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;

import org.jboss.jms.server.security.Role;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * This class allows the Configuration class to be configured via a config file.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class FileConfiguration extends Configuration
{

   private static final String READ_ATTR = "read";
   private static final String WRITE_ATTR = "write";
   private static final String CREATE_ATTR = "create";
   private static final String NAME_ATTR = "name";

   //default confog file location
   private String configurationUrl = "jbm-configuration.xml";

   public void start() throws Exception
   {

      URL url = getClass().getClassLoader().getResource(configurationUrl);
      Element e = XMLUtil.urlToElement(url);
      messagingServerID = getInteger(e, "server-peer-id", messagingServerID);
      _defaultQueueJNDIContext = getString(e, "default-queue-jndi-context", _defaultQueueJNDIContext);
      _defaultTopicJNDIContext = getString(e, "default-topic-jndi-context", _defaultTopicJNDIContext);
      _securityDomain = getString(e, "security-domain", _securityDomain);
      _messageCounterSamplePeriod = getLong(e, "message-counter-sample-period", _messageCounterSamplePeriod);
      _defaultMessageCounterHistoryDayLimit = getInteger(e, "default-message-counter-history-day-limit", _defaultMessageCounterHistoryDayLimit);
      _strictTck = getBoolean(e, "strict-tck", _strictTck);
      _postOfficeName = getString(e, "post-office-name", _postOfficeName);
      _clustered = getBoolean(e, "clustered", _clustered);
      _stateTimeout = getLong(e, "state-timeout", _stateTimeout);
      _castTimeout = getLong(e, "cast-timeout", _castTimeout);
      _groupName = getString(e, "group-name", _groupName);
      _controlChannelName = getString(e, "control-channel-name", _controlChannelName);
      _dataChannelName = getString(e, "data-channel-name", _dataChannelName);
      _channelPartitionName = getString(e, "channel-partition-name", _channelPartitionName);
      _remotingTransport = TransportType.valueOf(getString(e, "remoting-transport", _remotingTransport.name()));
      _remotingBindAddress = getInteger(e, "remoting-bind-address", _remotingBindAddress);
      _remotingTimeout = getInteger(e, "remoting-timeout", _remotingTimeout);
      _remotingDisableInvm = getBoolean(e, "remoting-disable-invm", _remotingDisableInvm);
      _remotingEnableSSL = getBoolean(e, "remoting-enable-ssl", _remotingEnableSSL);
      _remotingSSLKeyStorePath = getString(e, "remoting-ssl-keystore-path", _remotingSSLKeyStorePath);
      _remotingSSLKeyStorePassword = getString(e, "remoting-ssl-keystore-password", _remotingSSLKeyStorePassword);
      _remotingSSLTrustStorePath = getString(e, "remoting-ssl-truststore-path", _remotingSSLTrustStorePath);
      _remotingSSLTrustStorePassword = getString(e, "remoting-ssl-truststore-password", _remotingSSLTrustStorePassword);

      NodeList defaultInterceptors = e.getElementsByTagName("default-interceptors-config");

      ArrayList<String> interceptorList = new ArrayList<String>();
      if (defaultInterceptors.getLength() > 0)
      {

         NodeList interceptors = defaultInterceptors.item(0).getChildNodes();
         for (int k = 0; k < interceptors.getLength(); k++)
         {
            if ("interceptor".equalsIgnoreCase(interceptors.item(k).getNodeName()))
            {
               String clazz = interceptors.item(k).getAttributes().getNamedItem("class").getNodeValue();
               interceptorList.add(clazz);
            }
         }
      }
      this.defaultInterceptors = interceptorList;
   }

   private Boolean getBoolean(Element e, String name, Boolean def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return Boolean.valueOf(nl.item(0).getTextContent().trim());
      }
      return def;
   }

   private Integer getInteger(Element e, String name, Integer def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return Integer.valueOf(nl.item(0).getTextContent().trim());
      }
      return def;
   }

   private Long getLong(Element e, String name, Long def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return Long.valueOf(nl.item(0).getTextContent().trim());
      }
      return def;
   }

   private String getString(Element e, String name, String def)
   {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0)
      {
         return nl.item(0).getTextContent().trim();
      }
      return def;
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
