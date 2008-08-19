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

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * ConfigurationImpl
 * This class allows the Configuration class to be configured via a config file.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class FileConfiguration extends ConfigurationImpl
{
   private static final long serialVersionUID = -4766689627675039596L;
   
   private static final Logger log = Logger.getLogger(FileConfiguration.class);

   // Constants ------------------------------------------------------------------------
   
   private static final String DEFAULT_CONFIGURATION_URL = "jbm-configuration.xml";
   
   // Attributes ----------------------------------------------------------------------    
      
   private String configurationUrl = DEFAULT_CONFIGURATION_URL;

   // Public -------------------------------------------------------------------------
   
   public void start() throws Exception
   {
      URL url = getClass().getClassLoader().getResource(configurationUrl);
      Reader reader = new InputStreamReader(url.openStream());
      String xml = XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      Element e = XMLUtil.stringToElement(xml);

      clustered = getBoolean(e, "clustered", clustered);
      
      backup = getBoolean(e, "backup", backup);

      //NOTE! All the defaults come from the super class
      
      scheduledThreadPoolMaxSize = getInteger(e, "scheduled-max-pool-size", scheduledThreadPoolMaxSize);
      
      requireDestinations = getBoolean(e, "require-destinations", requireDestinations);
      
      securityEnabled = getBoolean(e, "security-enabled", securityEnabled);
      
      jmxManagementEnabled = getBoolean(e, "jmx-management-enabled", jmxManagementEnabled);
      
      securityInvalidationInterval = getLong(e, "security-invalidation-interval", securityInvalidationInterval);
            
      // Remoting config
      
      transport = TransportType.valueOf(getString(e, "remoting-transport", transport.toString()));
      
      host = getString(e, "remoting-host", host);

      if (System.getProperty("java.rmi.server.hostname") == null)
      {
         System.setProperty("java.rmi.server.hostname", host);
      }

      port = getInteger(e, "remoting-port", port);
      
      String sbackupTransport = getString(e, "remoting-backup-transport", null);
      
      if (sbackupTransport != null)
      {
         backupTransport = TransportType.valueOf(sbackupTransport);
      }
      
      backupHost = getString(e, "remoting-backup-host", backupHost);

      backupPort = getInteger(e, "remoting-backup-port", backupPort);
      
      int packetConfirmationBatchSize = getInteger(e, "packet-confirmation-batch-size", ConnectionParamsImpl.DEFAULT_PACKET_CONFIRMATION_BATCH_SIZE);

      int callTimeout = getInteger(e, "remoting-call-timeout", ConnectionParamsImpl.DEFAULT_CALL_TIMEOUT);

      boolean inVMOptimisationEnabled = getBoolean(e, "remoting-enable-invm-optimisation", ConnectionParamsImpl.DEFAULT_INVM_OPTIMISATION_ENABLED);

      boolean tcpNoDelay = getBoolean(e, "remoting-tcp-nodelay", ConnectionParamsImpl.DEFAULT_TCP_NODELAY);

      int tcpReceiveBufferSize = getInteger(e, "remoting-tcp-receive-buffer-size", ConnectionParamsImpl.DEFAULT_TCP_RECEIVE_BUFFER_SIZE);

      int tcpSendBufferSize = getInteger(e, "remoting-tcp-send-buffer-size", ConnectionParamsImpl.DEFAULT_TCP_SEND_BUFFER_SIZE);

      int pingInterval = getInteger(e, "remoting-ping-interval", ConnectionParamsImpl.DEFAULT_PING_INTERVAL);

      sslEnabled = getBoolean(e, "remoting-enable-ssl", ConnectionParamsImpl.DEFAULT_SSL_ENABLED);

      keyStorePath = getString(e, "remoting-ssl-keystore-path", ConfigurationImpl.DEFAULT_KEYSTORE_PATH);

      keyStorePassword = getString(e, "remoting-ssl-keystore-password", ConfigurationImpl.DEFAULT_KEYSTORE_PASSWORD);

      trustStorePath = getString(e, "remoting-ssl-truststore-path", ConfigurationImpl.DEFAULT_TRUSTSTORE_PATH);

      trustStorePassword = getString(e, "remoting-ssl-truststore-password", ConfigurationImpl.DEFAULT_TRUSTSTORE_PASSWORD);

      defaultConnectionParams.setCallTimeout(callTimeout);
      
      defaultConnectionParams.setInVMOptimisationEnabled(inVMOptimisationEnabled);
      
      defaultConnectionParams.setTcpNoDelay(tcpNoDelay);
      
      defaultConnectionParams.setTcpReceiveBufferSize(tcpReceiveBufferSize);
      
      defaultConnectionParams.setTcpSendBufferSize(tcpSendBufferSize);
      
      defaultConnectionParams.setPingInterval(pingInterval);
      
      defaultConnectionParams.setSSLEnabled(sslEnabled);
      
      defaultConnectionParams.setPacketConfirmationBatchSize(packetConfirmationBatchSize);
      
      NodeList interceptorNodes = e.getElementsByTagName("remoting-interceptors");

      ArrayList<String> interceptorList = new ArrayList<String>();

      if (interceptorNodes.getLength() > 0)
      {
         NodeList interceptors = interceptorNodes.item(0).getChildNodes();

         for (int k = 0; k < interceptors.getLength(); k++)
         {
            if ("class-name".equalsIgnoreCase(interceptors.item(k).getNodeName()))
            {
               String clazz = interceptors.item(k).getTextContent();
               
               interceptorList.add(clazz);
            }
         }
      }
      this.interceptorClassNames = interceptorList;
      
      NodeList acceptorFactoryNodes = e.getElementsByTagName("remoting-acceptor-factories");
      
      Set<String> acceptorFactories = new HashSet<String>();

      if (acceptorFactoryNodes.getLength() > 0)
      {
         NodeList factories = acceptorFactoryNodes.item(0).getChildNodes();

         for (int k = 0; k < factories.getLength(); k++)
         {
            if ("class-name".equalsIgnoreCase(factories.item(k).getNodeName()))
            {
               String clazz = factories.item(k).getTextContent();
               
               acceptorFactories.add(clazz);
            }
         }
      }
      this.acceptorFactoryClassNames = acceptorFactories;
      
    
      // Persistence config

      bindingsDirectory = getString(e, "bindings-directory", bindingsDirectory);

      createBindingsDir = getBoolean(e, "create-bindings-dir", createBindingsDir);

      journalDirectory = getString(e, "journal-directory", journalDirectory);

      createJournalDir = getBoolean(e, "create-journal-dir", createJournalDir);

      String s = getString(e, "journal-type", journalType.toString());

      if (s == null || (!s.equals(JournalType.NIO.toString()) && !s.equals(JournalType.ASYNCIO.toString()) && !s.equals(JournalType.JDBC.toString())))
      {
         throw new IllegalArgumentException("Invalid journal type " + s);
      }

      if (s.equals(JournalType.NIO.toString()))
      {
         journalType = JournalType.NIO;
      }
      else if (s.equals(JournalType.ASYNCIO.toString()))
      {
         journalType = JournalType.ASYNCIO;
      }
      else if (s.equals(JournalType.JDBC.toString()))
      {
         journalType = JournalType.JDBC;
      }

      journalSyncTransactional = getBoolean(e, "journal-sync-transactional", journalSyncTransactional);
      
      journalSyncNonTransactional = getBoolean(e, "journal-sync-non-transactional", journalSyncNonTransactional);

      journalFileSize = getInteger(e, "journal-file-size", journalFileSize);

      journalMinFiles = getInteger(e, "journal-min-files", journalMinFiles);

      journalMaxAIO = getInteger(e, "journal-max-aio", journalMaxAIO);
      
      
   }

   public String getConfigurationUrl()
   {
      return configurationUrl;
   }

   public void setConfigurationUrl(String configurationUrl)
   {
      this.configurationUrl = configurationUrl;
   }
   
   // Private -------------------------------------------------------------------------

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
