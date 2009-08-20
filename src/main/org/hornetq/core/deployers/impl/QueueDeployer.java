/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.deployers.impl;

import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.management.HornetQServerControl;
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
   private final HornetQServerControl serverControl;

   public QueueDeployer(final DeploymentManager deploymentManager, final HornetQServerControl serverControl)
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
      org.hornetq.utils.XMLUtil.validate(rootNode, "schema/hornetq-configuration.xsd");
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
      return new String[] { "hornetq-configuration.xml", "hornetq-queues.xml" };
   }

   private QueueConfiguration parseQueueConfiguration(final Node node)
   {
      String name = node.getAttributes().getNamedItem("name").getNodeValue();
      String address = null;
      String filterString = null;
      boolean durable = true;

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
