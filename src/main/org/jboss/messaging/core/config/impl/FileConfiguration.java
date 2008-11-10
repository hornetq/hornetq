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
import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
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
      
      queueActivationTimeout = getLong(e, "queue-activation-timeout", queueActivationTimeout);

      //NOTE! All the defaults come from the super class
      
      scheduledThreadPoolMaxSize = getInteger(e, "scheduled-max-pool-size", scheduledThreadPoolMaxSize);
      
      requireDestinations = getBoolean(e, "require-destinations", requireDestinations);
      
      securityEnabled = getBoolean(e, "security-enabled", securityEnabled);
      
      jmxManagementEnabled = getBoolean(e, "jmx-management-enabled", jmxManagementEnabled);
      
      securityInvalidationInterval = getLong(e, "security-invalidation-interval", securityInvalidationInterval);
      
      connectionScanPeriod = getLong(e, "connection-scan-period", connectionScanPeriod);

      transactionTimeout = getLong(e, "transaction-timeout", transactionTimeout);

      transactionTimeoutScanPeriod = getLong(e, "transaction-timeout-scan-period", transactionTimeoutScanPeriod);
      
      managementAddress = new SimpleString(getString(e, "management-address", managementAddress.toString()));
            
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
      
      NodeList backups = e.getElementsByTagName("backup-connector");
      
      //TODO  combine all these duplicated transport config parsing code - it's messy!
      if (backups.getLength() > 0)
      {
         Node backup = backups.item(0);
         
         NodeList children = backup.getChildNodes();
         
         String clazz = null;
         
         Map<String, Object> params = new HashMap<String, Object>();
                        
         for (int l = 0; l < children.getLength(); l++)
         {                                  
            String nodeName = children.item(l).getNodeName();
            
            if ("factory-class".equalsIgnoreCase(nodeName))
            {                    
               clazz = children.item(l).getTextContent();
            }
            else if ("params".equalsIgnoreCase(nodeName))
            {                                                             
               NodeList nlParams = children.item(l).getChildNodes();
               
               for (int m = 0; m < nlParams.getLength(); m++)
               {
                  if ("param".equalsIgnoreCase(nlParams.item(m).getNodeName()))
                  {
                     Node paramNode = nlParams.item(m);
                     
                     NamedNodeMap attributes = paramNode.getAttributes();
                     
                     Node nkey = attributes.getNamedItem("key");
                     
                     String key = nkey.getTextContent();
                     
                     Node nValue = attributes.getNamedItem("value");
                     
                     String value = nValue.getTextContent();
                     
                     Node nType = attributes.getNamedItem("type");
                     
                     String type = nType.getTextContent();
                     
                     if (type.equalsIgnoreCase("Integer"))
                     {
                        try
                        {
                           Integer iVal = Integer.parseInt(value);
                           
                           params.put(key, iVal);
                        }
                        catch (NumberFormatException e2)
                        {
                           throw new IllegalArgumentException("Remoting acceptor parameter " + value + " is not a valid Integer");
                        }
                     }
                     else if (type.equalsIgnoreCase("Long"))
                     {
                        try
                        {
                           Long lVal = Long.parseLong(value);
                           
                           params.put(key, lVal);
                        }
                        catch (NumberFormatException e2)
                        {
                           throw new IllegalArgumentException("Remoting acceptor parameter " + value + " is not a valid Long");
                        }
                     }
                     else if (type.equalsIgnoreCase("String"))
                     {
                        params.put(key, value);                             
                     }
                     else if (type.equalsIgnoreCase("Boolean"))
                     {
                        Boolean lVal = Boolean.parseBoolean(value);
                           
                        params.put(key, lVal);                              
                     }
                     else
                     {
                        throw new IllegalArgumentException("Invalid parameter type " + type);
                     }
                  }
               }
            }
            
            this.backupConnectorConfig = new TransportConfiguration(clazz, params);
         }

      }
      
      NodeList acceptorNodes = e.getElementsByTagName("remoting-acceptors");
      
      if (acceptorNodes.getLength() > 0)
      {
         NodeList acceptors = acceptorNodes.item(0).getChildNodes();
         
         for (int k = 0; k < acceptors.getLength(); k++)
         {
            if ("acceptor".equalsIgnoreCase(acceptors.item(k).getNodeName()))
            {
               NodeList children = acceptors.item(k).getChildNodes();
               
               String clazz = null;
               
               Map<String, Object> params = new HashMap<String, Object>();
                              
               for (int l = 0; l < children.getLength(); l++)
               {                                  
                  String nodeName = children.item(l).getNodeName();
                  
                  if ("factory-class".equalsIgnoreCase(nodeName))
                  {                    
                     clazz = children.item(l).getTextContent();
                  }
                  else if ("params".equalsIgnoreCase(nodeName))
                  {                                                             
                     NodeList nlParams = children.item(l).getChildNodes();
                     
                     for (int m = 0; m < nlParams.getLength(); m++)
                     {
                        if ("param".equalsIgnoreCase(nlParams.item(m).getNodeName()))
                        {
                           Node paramNode = nlParams.item(m);
                           
                           NamedNodeMap attributes = paramNode.getAttributes();
                           
                           Node nkey = attributes.getNamedItem("key");
                           
                           String key = nkey.getTextContent();
                           
                           Node nValue = attributes.getNamedItem("value");
                           
                           String value = nValue.getTextContent();
                           
                           Node nType = attributes.getNamedItem("type");
                           
                           String type = nType.getTextContent();
                           
                           if (type.equalsIgnoreCase("Integer"))
                           {
                              try
                              {
                                 Integer iVal = Integer.parseInt(value);
                                 
                                 params.put(key, iVal);
                              }
                              catch (NumberFormatException e2)
                              {
                                 throw new IllegalArgumentException("Remoting acceptor parameter " + value + " is not a valid Integer");
                              }
                           }
                           else if (type.equalsIgnoreCase("Long"))
                           {
                              try
                              {
                                 Long lVal = Long.parseLong(value);
                                 
                                 params.put(key, lVal);
                              }
                              catch (NumberFormatException e2)
                              {
                                 throw new IllegalArgumentException("Remoting acceptor parameter " + value + " is not a valid Long");
                              }
                           }
                           else if (type.equalsIgnoreCase("String"))
                           {
                              params.put(key, value);                             
                           }
                           else if (type.equalsIgnoreCase("Boolean"))
                           {
                              Boolean lVal = Boolean.parseBoolean(value);
                                 
                              params.put(key, lVal);                              
                           }
                           else
                           {
                              throw new IllegalArgumentException("Invalid parameter type " + type);
                           }
                        }
                     }
                  }                                                                  
               }
      
               TransportConfiguration info = new TransportConfiguration(clazz, params);
               
               acceptorConfigs.add(info);    
            }
         }
      }

      // Persistence config

      bindingsDirectory = getString(e, "bindings-directory", bindingsDirectory);

      createBindingsDir = getBoolean(e, "create-bindings-dir", createBindingsDir);

      journalDirectory = getString(e, "journal-directory", journalDirectory);
      
      pagingDirectory = getString(e, "paging-directory", pagingDirectory);
      
      pagingMaxGlobalSize = getLong(e, "paging-max-global-size-bytes", pagingMaxGlobalSize);

      createJournalDir = getBoolean(e, "create-journal-dir", createJournalDir);

      String s = getString(e, "journal-type", journalType.toString());

      if (s == null || (!s.equals(JournalType.NIO.toString()) && !s.equals(JournalType.ASYNCIO.toString())))
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
      
      journalSyncTransactional = getBoolean(e, "journal-sync-transactional", journalSyncTransactional);
      
      journalSyncNonTransactional = getBoolean(e, "journal-sync-non-transactional", journalSyncNonTransactional);

      journalFileSize = getInteger(e, "journal-file-size", journalFileSize);
      
      journalBufferReuseSize = getInteger(e, "journal-buffer-reuse-size", journalBufferReuseSize);

      journalMinFiles = getInteger(e, "journal-min-files", journalMinFiles);

      journalMaxAIO = getInteger(e, "journal-max-aio", journalMaxAIO);

      wildcardRoutingEnabled = getBoolean(e, "wild-card-routing-enabled", wildcardRoutingEnabled);

      messageCounterEnabled = getBoolean(e, "message-counter-enabled", messageCounterEnabled);
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
