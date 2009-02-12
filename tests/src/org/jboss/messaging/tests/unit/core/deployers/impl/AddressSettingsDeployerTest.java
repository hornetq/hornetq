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

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.AddressSettingsDeployer;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.XMLUtil;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class AddressSettingsDeployerTest extends TestCase
{
   private String conf = "<address-settings match=\"queues.*\">\n" +
           "      <manageConfirmations>false</manageConfirmations>\n" +
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
      repository = EasyMock.createStrictMock(HierarchicalRepository.class);
      DeploymentManager deploymentManager = EasyMock.createNiceMock(DeploymentManager.class);
      addressSettingsDeployer = new AddressSettingsDeployer(deploymentManager, repository);
   }

   public void testDeploy() throws Exception
   {
      final AddressSettings addressSettings = new AddressSettings();
      addressSettings.setClustered(false);
      addressSettings.setRedeliveryDelay((long) 100);
      addressSettings.setMaxSizeBytes(-100);
      addressSettings.setDistributionPolicyClass("org.jboss.messaging.core.impl.RoundRobinDistributionPolicy");
      addressSettings.setMessageCounterHistoryDayLimit(1000);
      addressSettings.setDeadLetterAddress(new SimpleString("DLQtest"));
      addressSettings.setExpiryAddress(new SimpleString("ExpiryQueueTest"));

      repository.addMatch(EasyMock.eq("queues.*"),(AddressSettings) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            AddressSettings q = (AddressSettings) EasyMock.getCurrentArguments()[1];
            assertFalse(q.isClustered());
            assertEquals(q.getRedeliveryDelay(), addressSettings.getRedeliveryDelay());
            assertEquals(q.getMaxSizeBytes(), addressSettings.getMaxSizeBytes());
            assertEquals(q.getDistributionPolicyClass(), addressSettings.getDistributionPolicyClass());
            assertEquals(q.getMessageCounterHistoryDayLimit(), addressSettings.getMessageCounterHistoryDayLimit());
            assertEquals(q.getDeadLetterAddress(), addressSettings.getDeadLetterAddress());
            assertEquals(q.getExpiryAddress(), addressSettings.getExpiryAddress());
            return null;
         }
      });
      EasyMock.replay(repository);
      addressSettingsDeployer.deploy(XMLUtil.stringToElement(conf));
      EasyMock.verify(repository);
   }

   public void testUndeploy() throws Exception
   {
      repository.removeMatch("queues.*");
      EasyMock.replay(repository);
      addressSettingsDeployer.undeploy(XMLUtil.stringToElement(conf));
      EasyMock.verify(repository);
   }


}
