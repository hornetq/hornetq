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

package org.hornetq.tests.integration.management;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * A ManagementTestBase
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public abstract class ManagementTestBase extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MBeanServer mbeanServer;

   // Static --------------------------------------------------------

   protected static void consumeMessages(int expected, ClientSession session, SimpleString queue) throws Exception
   {
      ClientConsumer consumer = null;
      try
      {
         consumer = session.createConsumer(queue);
         ClientMessage m = null;
         for (int i = 0; i < expected; i++)
         {
            m = consumer.receive(500);
            assertNotNull("expected to received " + expected + " messages, got only " + i, m);
            m.acknowledge();
         }
         session.commit();
         m = consumer.receive(500);
         assertNull("received one more message than expected (" + expected + ")", m);
      }
      finally
      {
         if (consumer != null)
         {
            consumer.close();
         }
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      mbeanServer = MBeanServerFactory.createMBeanServer();
   }
   
   protected void tearDown() throws Exception
   {
      MBeanServerFactory.releaseMBeanServer(mbeanServer);
      
      mbeanServer = null;
      
      super.tearDown();
   }

   protected void checkNoResource(ObjectName on)
   {
      assertFalse("unexpected resource for " + on, mbeanServer.isRegistered(on));
   }

   protected void checkResource(ObjectName on)
   {
      assertTrue("no resource for " + on, mbeanServer.isRegistered(on));
   }


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
