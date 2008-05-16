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

import static org.jboss.messaging.core.remoting.TransportType.TCP;

import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;

import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**ConfigurationImpl
 * This class allows the Configuration class to be configured via a config file.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class FileConfiguration extends ConfigurationImpl implements Serializable
{
	private static final long serialVersionUID = -4766689627675039596L;
	
	//default config file location
   private String configurationUrl = "jbm-configuration.xml";

   public void start() throws Exception
   {
      URL url = getClass().getClassLoader().getResource(configurationUrl);
      Reader reader = new InputStreamReader(url.openStream());
      String xml = XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      Element e = XMLUtil.stringToElement(xml);
      
      strictTck = getBoolean(e, "strict-tck", strictTck);
      
      clustered = getBoolean(e, "clustered", clustered);
      
      scheduledThreadPoolMaxSize = getInteger(e, "scheduled-executor-max-pool-size", scheduledThreadPoolMaxSize);
      
      transport = TransportType.valueOf(getString(e, "remoting-transport", TCP.name()));
      
      host = getString(e, "remoting-host", "localhost");

      if (System.getProperty("java.rmi.server.hostname") == null)
         System.setProperty("java.rmi.server.hostname", host);

      port = getInteger(e, "remoting-bind-address", DEFAULT_REMOTING_PORT);
      
      timeout = getInteger(e, "remoting-timeout", 5);
      
      invmDisabled = getBoolean(e, "remoting-disable-invm", false);
      
      tcpNoDelay = getBoolean(e, "remoting-tcp-nodelay", false);
      
      tcpReceiveBufferSize = getInteger(e, "remoting-tcp-receive-buffer-size", -1);

      tcpSendBufferSize = getInteger(e, "remoting-tcp-send-buffer-size", -1);
      
      writeQueueBlockTimeout = getLong(e, "remoting-writequeue-block-timeout", 10000L);
      
      writeQueueMinBytes = getLong(e, "remoting-writequeue-minbytes", 65536L);
      
      writeQueueMaxBytes = getLong(e, "remoting-writequeue-maxbytes", 1048576L);

      sslEnabled = getBoolean(e, "remoting-enable-ssl", false);
      
      keyStorePath = getString(e, "remoting-ssl-keystore-path", null);
      
      keyStorePassword = getString(e, "remoting-ssl-keystore-password", null);
      
      trustStorePath = getString(e, "remoting-ssl-truststore-path", null);
      
      trustStorePassword = getString(e, "remoting-ssl-truststore-password", null);

      requireDestinations = getBoolean(e, "require-destinations", requireDestinations);
      
      //Persistence config
      
      this.bindingsDirectory = getString(e, "bindings-directory", bindingsDirectory);
      
      this.createBindingsDir = getBoolean(e, "create-bindings-dir", createBindingsDir);
      
      this.journalDirectory = getString(e, "journal-directory", journalDirectory);
      
      this.createJournalDir = getBoolean(e, "create-journal-dir", createJournalDir);
      
      String s = getString(e, "journal-type", "nio");
      
      if (s == null || (!s.equals("nio") && !s.equals("asyncio") && !s.equals("jdbc")))
      {
      	throw new IllegalArgumentException("Invalid journal type " + s);
      }
      
      if (s.equals("nio"))
      {
      	journalType = JournalType.NIO;
      }
      else if (s.equals("asyncio"))
      {
      	journalType = JournalType.ASYNCIO;
      }
      else if (s.equals("jdbc"))
      {
      	journalType = JournalType.JDBC;
      }
      		
      this.journalSync = getBoolean(e, "journal-sync", true);
      
      this.journalFileSize = getInteger(e, "journal-file-size", 10 * 1024 * 1024);
      
      this.journalMinFiles = getInteger(e, "journal-min-files", 10);
      
      this.journalTaskPeriod = getLong(e, "journal-task-period", 5000L);
      
      this.securityEnabled = getBoolean(e, "security-enabled", true);
       
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
