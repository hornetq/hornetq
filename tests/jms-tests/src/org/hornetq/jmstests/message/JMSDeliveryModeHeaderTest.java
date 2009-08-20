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

package org.hornetq.jmstests.message;

import javax.jms.DeliveryMode;
import javax.jms.Message;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSDeliveryModeHeaderTest extends MessageHeaderTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testDefaultDeliveryMode() throws Exception
   {
      assertEquals(DeliveryMode.PERSISTENT, queueProducer.getDeliveryMode());
   }

   public void testNonPersistentDeliveryMode() throws Exception
   {
      queueProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      assertEquals(DeliveryMode.NON_PERSISTENT, queueProducer.getDeliveryMode());

      Message m = queueProducerSession.createMessage();
      queueProducer.send(m);

      assertEquals(DeliveryMode.NON_PERSISTENT, queueConsumer.receive().getJMSDeliveryMode());
   }

   public void testPersistentDeliveryMode() throws Exception
   {
      queueProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
      assertEquals(DeliveryMode.PERSISTENT, queueProducer.getDeliveryMode());

      Message m = queueProducerSession.createMessage();
      queueProducer.send(m);

      assertEquals(DeliveryMode.PERSISTENT, queueConsumer.receive().getJMSDeliveryMode());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
