/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.config.impl;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.deployers.impl.FileDeploymentManager;
import org.jboss.messaging.core.deployers.impl.QueueDeployer;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * A QueueDeployerTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 */
public class QueueDeployerTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testParseQueueConfigurationFromAddressSettings() throws Exception
   {
      Configuration configuration = new ConfigurationImpl();
      QueueDeployer deployer = new QueueDeployer(new FileDeploymentManager(500), configuration);

      String xml = "<settings xmlns='urn:jboss:messaging'>"
                 + "<queue name='foo' address='bar' filter='speed > 88' durable='false' />"
                 + "</settings>";
      
      Element rootNode = XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
      NodeList queueNodes = rootNode.getElementsByTagName("queue");
      assertEquals(1, queueNodes.getLength());
      deployer.deploy(queueNodes.item(0));
      
      assertEquals(1, configuration.getQueueConfigurations().size());
      
      QueueConfiguration queueConfiguration = configuration.getQueueConfigurations().get(0);
      assertEquals("foo", queueConfiguration.getName());
      assertEquals("bar", queueConfiguration.getAddress());
      assertEquals("speed > 88", queueConfiguration.getFilterString());
      assertEquals(false, queueConfiguration.isDurable());      
   }

   public void testParseQueueConfigurationFromJBMConfiguration() throws Exception
   {
      Configuration configuration = new ConfigurationImpl();
      QueueDeployer deployer = new QueueDeployer(new FileDeploymentManager(500), configuration);

      String xml = "<deployment xmlns='urn:jboss:messaging'> " 
                 + "<configuration>"
                 + "<acceptor><factory-class>FooAcceptor</factory-class></acceptor>"
                 + "<queue name='foo' address='bar' filter='speed > 88' durable='false' />"
                 + "</configuration>"
                 + "<settings>"
                 + "<queue name='foo2' address='bar2' filter='speed > 88' durable='true' />"
                 + "</settings>"
                 + "</deployment>";
      
      Element rootNode = XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
      NodeList queueNodes = rootNode.getElementsByTagName("queue");
      assertEquals(2, queueNodes.getLength());
      
      deployer.deploy(queueNodes.item(0));
      deployer.deploy(queueNodes.item(1));
      
      assertEquals(2, configuration.getQueueConfigurations().size());   
      assertEquals("foo", configuration.getQueueConfigurations().get(0).getName());
      assertEquals("foo2", configuration.getQueueConfigurations().get(1).getName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
