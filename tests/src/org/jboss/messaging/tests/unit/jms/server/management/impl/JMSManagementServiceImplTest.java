/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.jms.server.management.impl;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import junit.framework.TestCase;

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
import org.jboss.messaging.jms.server.management.impl.ConnectionFactoryControl;
import org.jboss.messaging.jms.server.management.impl.JMSManagementServiceImpl;
import org.jboss.messaging.jms.server.management.impl.JMSQueueControl;
import org.jboss.messaging.jms.server.management.impl.TopicControl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
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
      ObjectName objectName = JMSManagementServiceImpl
            .getJMSServerObjectName();
      ObjectInstance objectInstance = new ObjectInstance(objectName,
            JMSServerManager.class.getName());

      JMSServerManager server = createMock(JMSServerManager.class);

      MBeanServer mbeanServer = createMock(MBeanServer.class);
      expect(mbeanServer.isRegistered(objectName)).andReturn(false);
      expect(
            mbeanServer.registerMBean(isA(JMSServerControlMBean.class),
                  eq(objectName))).andReturn(objectInstance);

      replay(mbeanServer, server);

      JMSManagementService service = new JMSManagementServiceImpl(mbeanServer,
            true);
      service.registerJMSServer(server);
      
      verify(mbeanServer, server);
   }

   public void testRegisterAlreadyRegisteredJMSServer() throws Exception
   {
      ObjectName objectName = JMSManagementServiceImpl
            .getJMSServerObjectName();
      ObjectInstance objectInstance = new ObjectInstance(objectName,
            JMSServerManager.class.getName());

      JMSServerManager server = createMock(JMSServerManager.class);

      MBeanServer mbeanServer = createMock(MBeanServer.class);
      expect(mbeanServer.isRegistered(objectName)).andReturn(true);
      mbeanServer.unregisterMBean(objectName);
      expect(
            mbeanServer.registerMBean(isA(JMSServerControlMBean.class),
                  eq(objectName))).andReturn(objectInstance);

      replay(mbeanServer, server);

      JMSManagementService service = new JMSManagementServiceImpl(mbeanServer,
            true);
      service.registerJMSServer(server);
      
      verify(mbeanServer, server);
   }
   
   public void testRegisterQueue() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      ObjectName objectName = JMSManagementServiceImpl
            .getJMSQueueObjectName(name);
      ObjectInstance objectInstance = new ObjectInstance(objectName,
            JMSQueueControl.class.getName());

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      MBeanServer mbeanServer = createMock(MBeanServer.class);
      expect(mbeanServer.isRegistered(objectName)).andReturn(false);
      expect(
            mbeanServer.registerMBean(isA(JMSQueueControlMBean.class),
                  eq(objectName))).andReturn(objectInstance);

      replay(mbeanServer, coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSManagementService service = new JMSManagementServiceImpl(mbeanServer,
            true);
      service.registerQueue(queue, coreQueue, jndiBinding, postOffice, storageManager, queueSettingsRepository);

      verify(mbeanServer, coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testRegisterAlreadyRegisteredQueue() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      ObjectName objectName = JMSManagementServiceImpl
            .getJMSQueueObjectName(name);
      ObjectInstance objectInstance = new ObjectInstance(objectName,
            JMSQueueControl.class.getName());

      JBossQueue queue = new JBossQueue(name);
      Queue coreQueue = createMock(Queue.class);
      PostOffice postOffice = createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = createMock(HierarchicalRepository.class);

      MBeanServer mbeanServer = createMock(MBeanServer.class);
      expect(mbeanServer.isRegistered(objectName)).andReturn(true);
      mbeanServer.unregisterMBean(objectName);
      expect(
            mbeanServer.registerMBean(isA(JMSQueueControlMBean.class),
                  eq(objectName))).andReturn(objectInstance);

      replay(mbeanServer, coreQueue, postOffice, storageManager, queueSettingsRepository);

      JMSManagementService service = new JMSManagementServiceImpl(mbeanServer,
            true);
      service.registerQueue(queue, coreQueue, jndiBinding, postOffice, storageManager, queueSettingsRepository);

      verify(mbeanServer, coreQueue, postOffice, storageManager, queueSettingsRepository);
   }

   public void testRegisterTopic() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      ObjectName objectName = JMSManagementServiceImpl
            .getJMSTopicObjectName(name);
      ObjectInstance objectInstance = new ObjectInstance(objectName,
            TopicControl.class.getName());

      JBossTopic topic = new JBossTopic(name);
      PostOffice postOffice= createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);

      MBeanServer mbeanServer = createMock(MBeanServer.class);
      expect(mbeanServer.isRegistered(objectName)).andReturn(false);
      expect(
            mbeanServer.registerMBean(isA(TopicControlMBean.class),
                  eq(objectName))).andReturn(objectInstance);

      replay(mbeanServer, postOffice, storageManager);

      JMSManagementService service = new JMSManagementServiceImpl(mbeanServer,
            true);
      service.registerTopic(topic, jndiBinding, postOffice, storageManager);

      verify(mbeanServer, postOffice, storageManager);
   }

   public void testRegisterAlreadyRegisteredTopic() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      ObjectName objectName = JMSManagementServiceImpl
            .getJMSTopicObjectName(name);
      ObjectInstance objectInstance = new ObjectInstance(objectName,
            TopicControl.class.getName());

      JBossTopic topic = new JBossTopic(name);
      PostOffice postOffice= createMock(PostOffice.class);
      StorageManager storageManager = createMock(StorageManager.class);

      MBeanServer mbeanServer = createMock(MBeanServer.class);
      expect(mbeanServer.isRegistered(objectName)).andReturn(true);
      mbeanServer.unregisterMBean(objectName);
      expect(
            mbeanServer.registerMBean(isA(TopicControlMBean.class),
                  eq(objectName))).andReturn(objectInstance);

      replay(mbeanServer, postOffice, storageManager);

      JMSManagementService service = new JMSManagementServiceImpl(mbeanServer,
            true);
      service.registerTopic(topic, jndiBinding, postOffice, storageManager);

      verify(mbeanServer, postOffice, storageManager);
   }

   public void testRegisterConnectionFactory() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(jndiBinding);

      ObjectName objectName = JMSManagementServiceImpl
            .getConnectionFactoryObjectName(name);
      ObjectInstance objectInstance = new ObjectInstance(objectName,
            ConnectionFactoryControl.class.getName());

      JBossConnectionFactory connectionFactory = createMock(JBossConnectionFactory.class);     
      MBeanServer mbeanServer = createMock(MBeanServer.class);
      expect(mbeanServer.isRegistered(objectName)).andReturn(false);
      expect(
            mbeanServer.registerMBean(isA(ConnectionFactoryControlMBean.class),
                  eq(objectName))).andReturn(objectInstance);

      replay(mbeanServer, connectionFactory);

      JMSManagementService service = new JMSManagementServiceImpl(mbeanServer,
            true);
      service.registerConnectionFactory(name, connectionFactory, bindings);

      verify(mbeanServer, connectionFactory);
   }

   public void testRegisterAlreadyRegisteredConnectionFactory() throws Exception
   {
      String name = randomString();
      String jndiBinding = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(jndiBinding);

      ObjectName objectName = JMSManagementServiceImpl
            .getConnectionFactoryObjectName(name);
      ObjectInstance objectInstance = new ObjectInstance(objectName,
            ConnectionFactoryControl.class.getName());

      JBossConnectionFactory connectionFactory = createMock(JBossConnectionFactory.class);     
      MBeanServer mbeanServer = createMock(MBeanServer.class);
      expect(mbeanServer.isRegistered(objectName)).andReturn(true);
      mbeanServer.unregisterMBean(objectName);
      expect(
            mbeanServer.registerMBean(isA(ConnectionFactoryControlMBean.class),
                  eq(objectName))).andReturn(objectInstance);

      replay(mbeanServer, connectionFactory);

      JMSManagementService service = new JMSManagementServiceImpl(mbeanServer,
            true);
      service.registerConnectionFactory(name, connectionFactory, bindings);

      verify(mbeanServer, connectionFactory);
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
