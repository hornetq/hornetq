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
package org.jboss.jms.server.connectionfactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.jboss.jms.server.ServerPeer;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * This class will deploy any Connection Factories configured in the jbm-configuration.xml file.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConnectionFactoryDeployer
{
   private static final String JBM_FACTORIES_XML = "jbm-configuration.xml";
   private static final String FACTORY_ELEMENT = "factory";
   private static final String NAME_ATTR = "name";
   private static final String CLIENTID_ELEMENT = "client-id";
   private static final String PREFETECH_SIZE_ELEMENT = "prefetch-size";
   private static final String DEF_TEMP_Q_FULL_SIZE = "default-temp-queue-full-size";
   private static final String DEF_TEMP_Q_PAGE_SIZE_SIZE = "default-temp-queue-page-size";
   private static final String DEF_TEMP_Q_DOWN_CACHE_SIZE = "default-temp-queue-down-cache-size";
   private static final String DUPS_OK_BATCH_SIZE = "dups-ok-batch-size";
   private static final String SUPPORTS_FAILOVER = "supports-failover";
   private static final String SUPPORTS_LOAD_BALANCING = "supports-load-balancing";
   private static final String LOAD_BALANCING_FACTORY = "load-balancing-factory";
   private static final String STRICT_TCK = "strict-tck";
   private static final String DISABLE_REMOTING_CHECKS = "disable-remoting-checks";
   private static final String JNDI_BINDINGS = "jndi-bindings";
   private static final String BIDING = "binding";

   private List<ConnectionFactory> connectionFactories = new ArrayList<ConnectionFactory>();
   private ServerPeer serverPeer;
   private MinaService minaService;
//   private Connector connector;


   public ConnectionFactoryDeployer(ServerPeer serverPeer, MinaService minaService)
   {
      this.serverPeer = serverPeer;
      this.minaService = minaService;
   }

   /**
    * lifecycle method that will deploy and undeploy connection factories
    */
   public void start() throws Exception
   {
      //find the config file
      URL url = getClass().getClassLoader().getResource(JBM_FACTORIES_XML);
      Element e = XMLUtil.urlToElement(url);
      //lets get all the factories and create them
      NodeList children = e.getElementsByTagName(FACTORY_ELEMENT);
      for (int i = 0; i < children.getLength(); i++)
      {
         String clientId = null;
         String name = children.item(i).getAttributes().getNamedItem(NAME_ATTR).getNodeValue();
         ConnectionFactory connectionFactory = new ConnectionFactory(clientId);
         connectionFactory.setName(name);
         connectionFactories.add(connectionFactory);
         NodeList attributes = children.item(i).getChildNodes();
         for (int j = 0; j < attributes.getLength(); j++)
         {
            if (CLIENTID_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               clientId = attributes.item(j).getTextContent();
            }

            if (PREFETECH_SIZE_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setPrefetchSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
            }
            if (DEF_TEMP_Q_FULL_SIZE.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setDefaultTempQueueFullSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
            }
            if (DEF_TEMP_Q_PAGE_SIZE_SIZE.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setDefaultTempQueuePageSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
            }
            if (DEF_TEMP_Q_DOWN_CACHE_SIZE.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setDefaultTempQueueDownCacheSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
            }
            if (DUPS_OK_BATCH_SIZE.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setDupsOKBatchSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
            }
            if (SUPPORTS_FAILOVER.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setSupportsFailover(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
            }
            if (SUPPORTS_LOAD_BALANCING.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setSupportsLoadBalancing(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
            }
            if (LOAD_BALANCING_FACTORY.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setLoadBalancingFactory(attributes.item(j).getTextContent().trim());
            }
            if (STRICT_TCK.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setStrictTck(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
            }
            if (DISABLE_REMOTING_CHECKS.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setDisableRemotingChecks(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
            }
            if (JNDI_BINDINGS.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               connectionFactory.setJNDIBindings(getBindings(attributes.item(j).getChildNodes()));
            }

         }
         connectionFactory.setServerPeer(serverPeer);
         connectionFactory.setMinaService(minaService);
         connectionFactory.start();
      }
   }

   private List<String> getBindings(NodeList childNodes)
   {
      List<String> bindings = new ArrayList<String>();
      for (int i = 0; i < childNodes.getLength(); i++)
      {
         if (BIDING.equalsIgnoreCase(childNodes.item(i).getNodeName()))
         {
            bindings.add(childNodes.item(i).getTextContent().trim());
         }
      }
      return bindings;
   }

   /**
    * lifecycle method to stop factories
    *
    * @throws Exception
    */
   public void stop() throws Exception
   {
      for (ConnectionFactory connectionFactory : connectionFactories)
      {
         connectionFactory.stop();
      }
   }

}
