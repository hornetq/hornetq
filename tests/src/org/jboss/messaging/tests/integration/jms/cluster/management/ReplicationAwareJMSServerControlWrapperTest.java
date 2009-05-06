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

package org.jboss.messaging.tests.integration.jms.cluster.management;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.tests.integration.cluster.management.ReplicationAwareTestBase;
import org.jboss.messaging.tests.integration.jms.server.management.JMSUtil;
import org.jboss.messaging.tests.integration.jms.server.management.NullInitialContext;
import org.jboss.messaging.tests.integration.management.ManagementControlHelper;

/**
 * A ReplicationAwareQueueControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareJMSServerControlWrapperTest extends ReplicationAwareTestBase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicationAwareJMSServerControlWrapperTest.class);

   
   // Attributes ----------------------------------------------------

   private JMSServerManagerImpl liveServerManager;

   private JMSServerManagerImpl backupServerManager;

   private JMSServerControlMBean liveServerControl;

   private JMSServerControlMBean backupServerControl;

   // Static --------------------------------------------------------

   private static void checkNoResource(ObjectName on, MBeanServer mbeanServer)
   {
      assertFalse("unexpected resource for " + on, mbeanServer.isRegistered(on));
   }

   private static void checkResource(ObjectName on, MBeanServer mbeanServer)
   {
      assertTrue("no resource for " + on, mbeanServer.isRegistered(on));
   }

   // Public --------------------------------------------------------
   

   public void testCreateAndDestroyQueue() throws Exception
   {
      
      String name = randomString();
      String binding = randomString();
      ObjectName queueON = ObjectNames.getJMSQueueObjectName(name);
      
      checkNoResource(queueON, liveMBeanServer);
      checkNoResource(queueON, backupMBeanServer);
      
      liveServerControl.createQueue(name , binding);

      checkResource(queueON, liveMBeanServer);
      checkResource(queueON, backupMBeanServer);
      
      liveServerControl.destroyQueue(name);

      checkNoResource(queueON, liveMBeanServer);
      checkNoResource(queueON, backupMBeanServer);
   }
   
   public void testCreateAndDestroyTopic() throws Exception
   {
      
      String name = randomString();
      String binding = randomString();
      ObjectName topicON = ObjectNames.getJMSTopicObjectName(name);
      
      checkNoResource(topicON, liveMBeanServer);
      checkNoResource(topicON, backupMBeanServer);
      
      liveServerControl.createTopic(name , binding);

      checkResource(topicON, liveMBeanServer);
      checkResource(topicON, backupMBeanServer);
      
      liveServerControl.destroyTopic(name);

      checkNoResource(topicON, liveMBeanServer);
      checkNoResource(topicON, backupMBeanServer);
   }

   public void testListRemoteAddresses() throws Exception
   {
      assertEquals(0, liveServerControl.listRemoteAddresses().length);
      // the live server has opened a connection on the backup server
      assertEquals(1, backupServerControl.listRemoteAddresses().length);

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      assertEquals(1, liveServerControl.listRemoteAddresses().length);
      assertEquals(1, backupServerControl.listRemoteAddresses().length);
          
      connection.close();

      assertEquals(0, liveServerControl.listRemoteAddresses().length);
      assertEquals(1, backupServerControl.listRemoteAddresses().length);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   @Override
   protected void setUp() throws Exception
   {
      doSetup(false);

      backupServerManager = new JMSServerManagerImpl(backupServer);
      backupServerManager.setContext(new NullInitialContext());
      backupServerManager.start();
      
      liveServerManager = new JMSServerManagerImpl(liveServer);
      liveServerManager.setContext(new NullInitialContext());
      liveServerManager.start();
      
      liveServerControl = ManagementControlHelper.createJMSServerControl(liveMBeanServer);
      backupServerControl = ManagementControlHelper.createJMSServerControl(backupMBeanServer);      
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupServerManager.stop();
      liveServerManager.stop();
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
