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

import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A QueueDeployer
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class QueueDeployer extends XmlDeployer
{
   private final MessagingServerControlMBean serverControl;

   public QueueDeployer(final DeploymentManager deploymentManager, final MessagingServerControlMBean serverControl)
   {
      super(deploymentManager);

      this.serverControl = serverControl;
   }

   /**
    * the names of the elements to deploy
    * @return the names of the elements todeploy
    */
   public String[] getElementTagName()
   {
      return new String[] { "queue" };
   }

   @Override
   public void validate(Node rootNode) throws Exception
   {
      org.jboss.messaging.utils.XMLUtil.validate(rootNode, "schema/jbm-configuration.xsd");
   }

   /**
    * deploy an element
    * @param node the element to deploy
    * @throws Exception .
    */
   public void deploy(Node node) throws Exception
   {
      QueueConfiguration queueConfig = parseQueueConfiguration(node);

      serverControl.deployQueue(queueConfig.getAddress(),
                                queueConfig.getName(),
                                queueConfig.getFilterString(),
                                queueConfig.isDurable());
   }

   @Override
   public void undeploy(Node node) throws Exception
   {
      // Undeploy means nothing for core queues
   }

   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   public String[] getDefaultConfigFileNames()
   {
      return new String[] { "jbm-configuration.xml", "jbm-queues.xml" };
   }

   private QueueConfiguration parseQueueConfiguration(final Node node)
   {
      String name = node.getAttributes().getNamedItem("name").getNodeValue();
      String address = null;
      String filterString = null;
      boolean durable = false;

      NodeList children = node.getChildNodes();

      for (int j = 0; j < children.getLength(); j++)
      {
         Node child = children.item(j);

         if (child.getNodeName().equals("address"))
         {
            address = child.getTextContent().trim();
         }
         else if (child.getNodeName().equals("filter"))
         {
            filterString = child.getAttributes().getNamedItem("string").getNodeValue();
         }
         else if (child.getNodeName().equals("durable"))
         {
            durable = Boolean.parseBoolean(child.getTextContent().trim());
         }
      }

      return new QueueConfiguration(address, name, filterString, durable);
   }

}
