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
package org.jboss.jms.server.destination.settings.test.unit;

import junit.framework.TestCase;
import org.easymock.EasyMock;
import org.jboss.messaging.core.QueueSettings;
import org.jboss.messaging.deployers.queue.QueueSettingsDeployer;
import org.jboss.messaging.util.HierarchicalRepository;
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
           "      <max-size>-100</max-size>\n" +
           "      <distribution-policy-class>org.jboss.messaging.core.impl.RoundRobinDistributionPolicy</distribution-policy-class>\n" +
           "      <message-counter-history-day-limit>1000</message-counter-history-day-limit>\n" +
           "   </queue-settings>";

   QueueSettingsDeployer queueSettingsDeployer;


   protected void setUp() throws Exception
   {
      queueSettingsDeployer = new QueueSettingsDeployer();
   }

   public void testSimple() throws Exception
   {
      HierarchicalRepository<QueueSettings> repository = EasyMock.createStrictMock(HierarchicalRepository.class);
      queueSettingsDeployer.setQueueSettingsRepository(repository);
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setClustered(false);
      queueSettings.setDLQ("DLQtest");
      queueSettings.setExpiryQueue("ExpiryQueueTest");
      queueSettings.setRedeliveryDelay(100);
      queueSettings.setMaxSize(-100);
      queueSettings.setDistributionPolicyClass("org.jboss.messaging.core.impl.RoundRobinDistributionPolicy");
      queueSettings.setMessageCounterHistoryDayLimit(1000);
      repository.addMatch("queues.*", queueSettings);
      EasyMock.replay(repository);
      queueSettingsDeployer.deploy(XMLUtil.stringToElement(conf));
   }
}
