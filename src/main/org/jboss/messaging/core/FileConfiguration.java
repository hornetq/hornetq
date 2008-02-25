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
	private static final long serialVersionUID = -4766689627675039596L;
	
	//default config file location
   private String configurationUrl = "jbm-configuration.xml";

   public void start() throws Exception
   {
      URL url = getClass().getClassLoader().getResource(configurationUrl);
      
      Element e = XMLUtil.urlToElement(url);
      
      messagingServerID = getInteger(e, "server-peer-id", messagingServerID);
      
      securityDomain = getString(e, "security-domain", securityDomain);
      
      messageCounterSamplePeriod = getLong(e, "message-counter-sample-period", messageCounterSamplePeriod);
      
      defaultMessageCounterHistoryDayLimit = getInteger(e, "default-message-counter-history-day-limit", defaultMessageCounterHistoryDayLimit);
      
      strictTck = getBoolean(e, "strict-tck", strictTck);
      
      clustered = getBoolean(e, "clustered", clustered);
      
      scheduledThreadPoolMaxSize = getInteger(e, "scheduled-executor-max-pool-size", scheduledThreadPoolMaxSize);
      
      remotingTransport = TransportType.valueOf(getString(e, "remoting-transport", remotingTransport.name()));
      
      remotingBindAddress = getInteger(e, "remoting-bind-address", remotingBindAddress);
      
      remotingTimeout = getInteger(e, "remoting-timeout", remotingTimeout);
      
      remotingDisableInvm = getBoolean(e, "remoting-disable-invm", remotingDisableInvm);
      
      remotingEnableSSL = getBoolean(e, "remoting-enable-ssl", remotingEnableSSL);
      
      remotingSSLKeyStorePath = getString(e, "remoting-ssl-keystore-path", remotingSSLKeyStorePath);
      
      remotingSSLKeyStorePassword = getString(e, "remoting-ssl-keystore-password", remotingSSLKeyStorePassword);
      
      remotingSSLTrustStorePath = getString(e, "remoting-ssl-truststore-path", remotingSSLTrustStorePath);
      
      remotingSSLTrustStorePassword = getString(e, "remoting-ssl-truststore-password", remotingSSLTrustStorePassword);

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
   
   public String getConfigurationUrl()
   {
      return configurationUrl;
   }

   public void setConfigurationUrl(String configurationUrl)
   {
      this.configurationUrl = configurationUrl;
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
}
