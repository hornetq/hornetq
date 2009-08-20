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

package org.hornetq.tests.integration.jms.cluster.management;

import static org.hornetq.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ObjectNames;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.jms.server.management.JMSServerControl;
import org.hornetq.tests.integration.cluster.management.ReplicationAwareTestBase;
import org.hornetq.tests.integration.jms.server.management.JMSUtil;
import org.hornetq.tests.integration.jms.server.management.NullInitialContext;
import org.hornetq.tests.integration.management.ManagementControlHelper;

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

   private JMSServerControl liveServerControl;

   private JMSServerControl backupServerControl;

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

      liveServerControl.createQueue(name, binding);

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

      liveServerControl.createTopic(name, binding);

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

      liveServerManager = null;

      backupServerManager = null;

      liveServerControl = null;

      backupServerControl = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
