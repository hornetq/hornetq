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

package org.jboss.messaging.core.deployers.impl;

import java.util.List;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Node;

/**
 * A QueueDeployer
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class QueueDeployer extends XmlDeployer
{   
   private final Configuration serverConfiguration;

   public QueueDeployer(final DeploymentManager deploymentManager,
                                final Configuration configuration)
   {
      super(deploymentManager);
      this.serverConfiguration = configuration;
   }
   
   /**
    * the names of the elements to deploy
    * @return the names of the elements todeploy
    */
   public String[] getElementTagName()
   {
      return new String[]{"queue"};
   }

   @Override
   public void validate(Node rootNode) throws Exception
   {
      if ("deployment".equals(rootNode.getNodeName()))
      {
         XMLUtil.validate(rootNode, "jbm-configuration.xsd");
      } else 
      {
         XMLUtil.validate(rootNode, "queues.xsd");         
      }
   }

   /**
    * deploy an element
    * @param node the element to deploy
    * @throws Exception .
    */
   public void deploy(Node node) throws Exception
   {
      QueueConfiguration queueConfig = parseQueueConfiguration(node);
      List<QueueConfiguration> configurations = serverConfiguration.getQueueConfigurations();
      configurations.add(queueConfig);
      serverConfiguration.setQueueConfigurations(configurations);
   }
   
   @Override
   public void undeploy(Node node) throws Exception
   {
   }

   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   public String[] getConfigFileNames()
   {
      return new String[] {"jbm-configuration.xml", "queues.xml"};
   }

   private QueueConfiguration parseQueueConfiguration(final Node node)
   {
      String name = node.getAttributes().getNamedItem("name").getNodeValue();
      
      String address = node.getAttributes().getNamedItem("address").getNodeValue();

      String filterString = null;

      Node filterNode = node.getAttributes().getNamedItem("filter");
      if (filterNode !=null)
      {
         String filterValue = filterNode.getNodeValue();
         if (!"".equals(filterValue.trim()))
         {
            filterString = filterValue;
         }
      }

      boolean durable = false;
      Node durableNode = node.getAttributes().getNamedItem("durable");
      if (durableNode != null)
      {
         durable = Boolean.parseBoolean(durableNode.getNodeValue());
      }
      
      return new QueueConfiguration(address, name, filterString, durable);
   }

}
