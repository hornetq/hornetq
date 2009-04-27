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

package org.jboss.messaging.tests.unit.core.deployers.impl;

import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.AddressSettingsDeployer;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.settings.impl.HierarchicalObjectRepository;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class AddressSettingsDeployerTest extends UnitTestCase
{
   private String conf = "<address-settings match=\"queues.*\">\n" +
           "      <dead-letter-address>DLQtest</dead-letter-address>\n" +
           "      <expiry-address>ExpiryQueueTest</expiry-address>\n" +
           "      <redelivery-delay>100</redelivery-delay>\n" +
           "      <max-size-bytes>-100</max-size-bytes>\n" +
           "      <distribution-policy-class>org.jboss.messaging.core.impl.RoundRobinDistributionPolicy</distribution-policy-class>\n" +
           "      <message-counter-history-day-limit>1000</message-counter-history-day-limit>\n" +
           "   </address-settings>";

   private AddressSettingsDeployer addressSettingsDeployer;

   private HierarchicalRepository<AddressSettings> repository;

   protected void setUp() throws Exception
   {
      super.setUp();
      
      repository = new HierarchicalObjectRepository<AddressSettings>();
      DeploymentManager deploymentManager = new FakeDeploymentManager();
      addressSettingsDeployer = new AddressSettingsDeployer(deploymentManager, repository);
   }

   public void testDeploy() throws Exception
   {
      addressSettingsDeployer.deploy(XMLUtil.stringToElement(conf));
      AddressSettings as = repository.getMatch("queues.aq");
      assertNotNull(as);
      assertEquals(100, as.getRedeliveryDelay());
      assertEquals(-100, as.getMaxSizeBytes());
      assertEquals("org.jboss.messaging.core.impl.RoundRobinDistributionPolicy", as.getDistributionPolicyClass());
      assertEquals(1000, as.getMessageCounterHistoryDayLimit());
      assertEquals(new SimpleString("DLQtest"), as.getDeadLetterAddress());
      assertEquals(new SimpleString("ExpiryQueueTest"), as.getExpiryAddress());
   }
   
   public void testDeployFromConfigurationFile() throws Exception
   {
      String xml = "<deployment xmlns='urn:jboss:messaging'> " 
                 + "<configuration>"
                 + "<acceptors>"
                 + "<acceptor><factory-class>FooAcceptor</factory-class></acceptor>"
                 + "</acceptors>"
                 + "</configuration>"
                 + "<settings>"
                 + "   <address-settings match=\"queues.*\">"
                 + "      <dead-letter-address>DLQtest</dead-letter-address>\n"
                 + "      <expiry-address>ExpiryQueueTest</expiry-address>\n"
                 + "      <redelivery-delay>100</redelivery-delay>\n"
                 + "      <max-size-bytes>-100</max-size-bytes>\n"
                 + "      <distribution-policy-class>org.jboss.messaging.core.impl.RoundRobinDistributionPolicy</distribution-policy-class>"
                 + "      <message-counter-history-day-limit>1000</message-counter-history-day-limit>"
                 + "   </address-settings>"
                 + "</settings>"
                 + "</deployment>";
      
      Element rootNode = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);
      addressSettingsDeployer.validate(rootNode);
      NodeList addressSettingsNode = rootNode.getElementsByTagName("address-settings");
      assertEquals(1, addressSettingsNode.getLength());

      addressSettingsDeployer.deploy(addressSettingsNode.item(0));
      AddressSettings as = repository.getMatch("queues.aq");
      assertNotNull(as);
      assertEquals(100, as.getRedeliveryDelay());
      assertEquals(-100, as.getMaxSizeBytes());
      assertEquals("org.jboss.messaging.core.impl.RoundRobinDistributionPolicy", as.getDistributionPolicyClass());
      assertEquals(1000, as.getMessageCounterHistoryDayLimit());
      assertEquals(new SimpleString("DLQtest"), as.getDeadLetterAddress());
      assertEquals(new SimpleString("ExpiryQueueTest"), as.getExpiryAddress());
   }

   public void testUndeploy() throws Exception
   {
      addressSettingsDeployer.deploy(XMLUtil.stringToElement(conf));
      AddressSettings as = repository.getMatch("queues.aq");
      assertNotNull(as);
      addressSettingsDeployer.undeploy(XMLUtil.stringToElement(conf));
      as = repository.getMatch("queues.aq");
      assertNull(as);
   }

}
