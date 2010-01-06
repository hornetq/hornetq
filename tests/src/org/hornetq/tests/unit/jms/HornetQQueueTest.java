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

package org.hornetq.tests.unit.jms;

import junit.framework.Assert;

import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class HornetQQueueTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testIsTemporary() throws Exception
   {
      HornetQQueue queue = (HornetQQueue) HornetQJMSClient.createHornetQQueue(RandomUtil.randomString());
      Assert.assertFalse(queue.isTemporary());
   }

   public void testGetQueueName() throws Exception
   {
      String queueName = RandomUtil.randomString();
      HornetQQueue queue = (HornetQQueue) HornetQJMSClient.createHornetQQueue(queueName);
      Assert.assertEquals(queueName, queue.getQueueName());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
