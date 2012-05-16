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

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.server.HornetQServer;
import org.w3c.dom.Node;

/**
 * A QueueDeployer
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class QueueDeployer extends XmlDeployer
{
   private final HornetQServer server;

   private final FileConfigurationParser parser = new FileConfigurationParser();

   public QueueDeployer(final DeploymentManager deploymentManager, final HornetQServer server)
   {
      super(deploymentManager);

      this.server = server;
   }

   /**
    * the names of the elements to deploy
    * @return the names of the elements todeploy
    */
   @Override
   public String[] getElementTagName()
   {
      return new String[] { "queue" };
   }

   @Override
   public void validate(final Node rootNode) throws Exception
   {
      org.hornetq.utils.XMLUtil.validate(rootNode, "schema/hornetq-configuration.xsd");
   }

   /**
    * deploy an element
    * @param node the element to deploy
    * @throws Exception
    */
   @Override
   public void deploy(final Node node) throws Exception
   {
      CoreQueueConfiguration queueConfig = parser.parseQueueConfiguration(node);

      server.deployQueue(SimpleString.toSimpleString(queueConfig.getAddress()),
                         SimpleString.toSimpleString(queueConfig.getName()),
                         SimpleString.toSimpleString(queueConfig.getFilterString()),
                         queueConfig.isDurable(),
                         false);
   }

   @Override
   public void undeploy(final Node node) throws Exception
   {
      // Undeploy means nothing for core queues
   }

   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   @Override
   public String[] getDefaultConfigFileNames()
   {
      return new String[] { "hornetq-configuration.xml", "hornetq-queues.xml" };
   }

}
