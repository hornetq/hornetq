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

package org.hornetq.tests.integration.cluster.management;

import static org.hornetq.tests.integration.management.ManagementControlHelper.createMessagingServerControl;
import static org.hornetq.tests.integration.management.ManagementControlHelper.createQueueControl;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomPositiveLong;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import javax.management.ObjectName;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.management.MessageCounterInfo;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.management.ObjectNames;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.SimpleString;

/**
 * A ReplicationAwareQueueControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareMessagingServerControlWrapperTest extends ReplicationAwareTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private SimpleString address;

   private ClientSession session;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCreateQueue() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString name = randomSimpleString();

      HornetQServerControl liveServerControl = createMessagingServerControl(liveMBeanServer);
      ObjectName queueON = ObjectNames.getQueueObjectName(address, name);

      assertResourceNotExists(liveMBeanServer, queueON);
      assertResourceNotExists(backupMBeanServer, queueON);

      liveServerControl.createQueue(address.toString(), name.toString());

      assertResourceExists(liveMBeanServer, queueON);
      assertResourceExists(backupMBeanServer, queueON);
   }

   public void testDestroyQueue() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString name = randomSimpleString();

      HornetQServerControl liveServerControl = createMessagingServerControl(liveMBeanServer);
      ObjectName queueON = ObjectNames.getQueueObjectName(address, name);

      assertResourceNotExists(liveMBeanServer, queueON);
      assertResourceNotExists(backupMBeanServer, queueON);

      // create the queue...
      liveServerControl.createQueue(address.toString(), name.toString());

      assertResourceExists(liveMBeanServer, queueON);
      assertResourceExists(backupMBeanServer, queueON);

      // ... and destroy it
      liveServerControl.destroyQueue(name.toString());

      assertResourceNotExists(liveMBeanServer, queueON);
      assertResourceNotExists(backupMBeanServer, queueON);
   }

   public void testEnableMessageCounters() throws Exception
   {
      HornetQServerControl liveServerControl = createMessagingServerControl(liveMBeanServer);
      HornetQServerControl backupServerControl = createMessagingServerControl(backupMBeanServer);

      assertFalse(liveServerControl.isMessageCounterEnabled());
      assertFalse(backupServerControl.isMessageCounterEnabled());

      liveServerControl.enableMessageCounters();

      assertTrue(liveServerControl.isMessageCounterEnabled());
      assertTrue(backupServerControl.isMessageCounterEnabled());
   }

   public void testDisableMessageCounters() throws Exception
   {
      HornetQServerControl liveServerControl = createMessagingServerControl(liveMBeanServer);
      HornetQServerControl backupServerControl = createMessagingServerControl(backupMBeanServer);

      assertFalse(liveServerControl.isMessageCounterEnabled());
      assertFalse(backupServerControl.isMessageCounterEnabled());

      // enable the counters...
      liveServerControl.enableMessageCounters();

      assertTrue(liveServerControl.isMessageCounterEnabled());
      assertTrue(backupServerControl.isMessageCounterEnabled());

      // and disable them
      liveServerControl.disableMessageCounters();

      assertFalse(liveServerControl.isMessageCounterEnabled());
      assertFalse(backupServerControl.isMessageCounterEnabled());
   }

   public void testResetAllMessageCounters() throws Exception
   {
      HornetQServerControl liveServerControl = createMessagingServerControl(liveMBeanServer);
      liveServerControl.enableMessageCounters();
      liveServerControl.setMessageCounterSamplePeriod(2000);

      QueueControl liveQueueControl = createQueueControl(address, address, liveMBeanServer);
      QueueControl backupQueueControl = createQueueControl(address, address, backupMBeanServer);

      // send on queue
      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createClientMessage(false);
      SimpleString key = randomSimpleString();
      long value = randomLong();
      message.putLongProperty(key, value);
      producer.send(message);

      Thread.sleep(liveServerControl.getMessageCounterSamplePeriod() * 2);

      // check the count is to 1 on both live & backup nodes
      String jsonString = liveQueueControl.listMessageCounter();
      MessageCounterInfo counter = MessageCounterInfo.fromJSON(jsonString);
      
      assertEquals(1, counter.getCount());
      counter = MessageCounterInfo.fromJSON(backupQueueControl.listMessageCounter());
      assertEquals(1, counter.getCount());

      liveServerControl.resetAllMessageCounters();
      Thread.sleep(liveServerControl.getMessageCounterSamplePeriod() * 2);

      // check the count has been reset to 0 on both live & backup nodes
      counter = MessageCounterInfo.fromJSON(liveQueueControl.listMessageCounter());
      assertEquals(0, counter.getCount());
      counter = MessageCounterInfo.fromJSON(backupQueueControl.listMessageCounter());
      assertEquals(0, counter.getCount());      
   }

   public void testSetMessageCounterSamplePeriod() throws Exception
   {
      long newPeriod = randomPositiveLong();

      HornetQServerControl liveServerControl = createMessagingServerControl(liveMBeanServer);
      HornetQServerControl backupServerControl = createMessagingServerControl(backupMBeanServer);

      assertEquals(liveServerControl.getMessageCounterSamplePeriod(),
                   backupServerControl.getMessageCounterSamplePeriod());

      liveServerControl.setMessageCounterSamplePeriod(newPeriod);

      assertEquals(newPeriod, liveServerControl.getMessageCounterSamplePeriod());
      assertEquals(newPeriod, backupServerControl.getMessageCounterSamplePeriod());
   }

   public void testSetMessageCounterMaxDayCount() throws Exception
   {
      int newCount = RandomUtil.randomPositiveInt();

      HornetQServerControl liveServerControl = createMessagingServerControl(liveMBeanServer);
      HornetQServerControl backupServerControl = createMessagingServerControl(backupMBeanServer);

      assertEquals(liveServerControl.getMessageCounterMaxDayCount(), backupServerControl.getMessageCounterMaxDayCount());

      liveServerControl.setMessageCounterMaxDayCount(newCount);

      assertEquals(newCount, liveServerControl.getMessageCounterMaxDayCount());
      assertEquals(newCount, backupServerControl.getMessageCounterMaxDayCount());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      address = RandomUtil.randomSimpleString();

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()),
                                                                     new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                                backupParams));

      session = sf.createSession(false, true, true);

      session.createQueue(address, address, null, false);
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();
      
      session = null;
      
      address = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
