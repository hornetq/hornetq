/*
 * JBoss, Home of Professional Open Source Copyright 2008, Red Hat Middleware
 * LLC, and individual contributors by the @authors tag. See the copyright.txt
 * in the distribution for a full listing of individual contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.unit.jms.server.management.impl;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.List;

import javax.management.ObjectName;

import junit.framework.TestCase;

import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.management.ConnectionFactoryControlMBean;
import org.jboss.messaging.jms.server.management.JMSManagementService;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.jms.server.management.impl.JMSManagementServiceImpl;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class JMSManagementServiceImplTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testRegisterJMSServer() throws Exception
   {
      ObjectName objectName = JMSManagementServiceImpl.getJMSServerObjectName();

      JMSServerManager server = createMock(JMSServerManager.class);

      ManagementService managementService = createMock(ManagementService.class);
      managementService.registerResource(eq(objectName), isA(JMSServerControlMBean.class));
      replay(managementService, server);

      JMSManagementService service = new JMSManagementServiceImpl(managementService);
      service.registerJMSServer(server);

      verify(managementService, server);
   }

   public void testRegisterQueue() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      ObjectName objectName = JMSManagementServiceImpl.getJMSQueueObjectName(name);

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      expect(coreQueue.isDurable()).andReturn(true);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      ManagementService managementService = createMock(ManagementService.class);
      MessageCounterManager messageCounterManager = createMock(MessageCounterManager.class);
      expect(managementService.getMessageCounterManager()).andReturn(messageCounterManager );
      expect(messageCounterManager.getMaxDayCount()).andReturn(randomPositiveInt());
      messageCounterManager.registerMessageCounter(eq(name), isA(MessageCounter.class));
      managementService.registerResource(eq(objectName), isA(JMSQueueControlMBean.class));
      
      replay(managementService, messageCounterManager, coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSManagementService service = new JMSManagementServiceImpl(managementService);
      service.registerQueue(queue, coreQueue, jndiBinding, postOffice, storageManager, queueSettingsRepository);

      verify(managementService, messageCounterManager, coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testRegisterTopic() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      ObjectName objectName = JMSManagementServiceImpl.getJMSTopicObjectName(name);

      JBossTopic topic = new JBossTopic(name);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);

      ManagementService managementService = createMock(ManagementService.class);
      managementService.registerResource(eq(objectName), isA(TopicControlMBean.class));

      replay(managementService, postOffice, storageManager);

      JMSManagementService service = new JMSManagementServiceImpl(managementService);
      service.registerTopic(topic, jndiBinding, postOffice, storageManager);
      
      verify(managementService, postOffice, storageManager);
   }

   public void testRegisterConnectionFactory() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(jndiBinding);

      ObjectName objectName = JMSManagementServiceImpl.getConnectionFactoryObjectName(name);

      JBossConnectionFactory connectionFactory = createMock(JBossConnectionFactory.class);
      ManagementService managementService = createMock(ManagementService.class);
      managementService.registerResource(eq(objectName), isA(ConnectionFactoryControlMBean.class));

      replay(managementService, connectionFactory);

      JMSManagementService service = new JMSManagementServiceImpl(managementService);
      service.registerConnectionFactory(name, connectionFactory, bindings);

      verify(managementService, connectionFactory);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
