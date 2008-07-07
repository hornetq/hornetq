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
import org.jboss.messaging.core.deployers.impl.QueueSettingsDeployer;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.XMLUtil;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueSettingsDeployerTest extends TestCase
{
   private String conf = "<queue-settings match=\"queues.*\">\n" +
           "      <clustered>false</clustered>\n" +
           "      <dlq>DLQtest</dlq>\n" +
           "      <expiry-queue>ExpiryQueueTest</expiry-queue>\n" +
           "      <redelivery-delay>100</redelivery-delay>\n" +
           "      <max-size-bytes>-100</max-size-bytes>\n" +
           "      <distribution-policy-class>org.jboss.messaging.core.impl.RoundRobinDistributionPolicy</distribution-policy-class>\n" +
           "      <message-counter-history-day-limit>1000</message-counter-history-day-limit>\n" +
           "   </queue-settings>";

   private QueueSettingsDeployer queueSettingsDeployer;

   private HierarchicalRepository<QueueSettings> repository;

   protected void setUp() throws Exception
   {
      repository = EasyMock.createStrictMock(HierarchicalRepository.class);
      DeploymentManager deploymentManager = EasyMock.createNiceMock(DeploymentManager.class);
      queueSettingsDeployer = new QueueSettingsDeployer(deploymentManager, repository);
   }

   public void testDeploy() throws Exception
   {
      final QueueSettings queueSettings = new QueueSettings();
      queueSettings.setClustered(false);
      queueSettings.setRedeliveryDelay((long) 100);
      queueSettings.setMaxSizeBytes(-100);
      queueSettings.setDistributionPolicyClass("org.jboss.messaging.core.impl.RoundRobinDistributionPolicy");
      queueSettings.setMessageCounterHistoryDayLimit(1000);
      queueSettings.setDLQ(new SimpleString("DLQtest"));
      queueSettings.setExpiryQueue(new SimpleString("ExpiryQueueTest"));

      repository.addMatch(EasyMock.eq("queues.*"),(QueueSettings) EasyMock.anyObject());
      EasyMock.expectLastCall().andAnswer(new IAnswer<Object>()
      {
         public Object answer() throws Throwable
         {
            QueueSettings q = (QueueSettings) EasyMock.getCurrentArguments()[1];
            assertFalse(q.isClustered());
            assertEquals(q.getRedeliveryDelay(), queueSettings.getRedeliveryDelay());
            assertEquals(q.getMaxSizeBytes(), queueSettings.getMaxSizeBytes());
            assertEquals(q.getDistributionPolicyClass(), queueSettings.getDistributionPolicyClass());
            assertEquals(q.getMessageCounterHistoryDayLimit(), queueSettings.getMessageCounterHistoryDayLimit());
            assertEquals(q.getDLQ(), queueSettings.getDLQ());
            assertEquals(q.getExpiryQueue(), queueSettings.getExpiryQueue());
            return null;
         }
      });
      EasyMock.replay(repository);
      queueSettingsDeployer.deploy(XMLUtil.stringToElement(conf));
      EasyMock.verify(repository);
   }

   public void testUndeploy() throws Exception
   {
      repository.removeMatch("queues.*");
      EasyMock.replay(repository);
      queueSettingsDeployer.undeploy(XMLUtil.stringToElement(conf));
      EasyMock.verify(repository);
   }


}
