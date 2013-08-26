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

package org.hornetq.jms.tests.message.foreign;
import org.junit.Before;
import org.junit.After;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import org.hornetq.jms.tests.message.SimpleJMSObjectMessage;
import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 * Tests the delivery/receipt of a foreign object message
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 *
 *
 */
public class ForeignObjectMessageTest extends ForeignMessageTest
{
   private ForeignTestObject testObj;

   @Override
   @Before
   public void setUp() throws Exception
   {
      testObj = new ForeignTestObject("hello", 2.2D);
      super.setUp();

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
      testObj = null;
   }

   @Override
   protected Message createForeignMessage() throws Exception
   {
      SimpleJMSObjectMessage m = new SimpleJMSObjectMessage();

      log.debug("creating JMS Message type " + m.getClass().getName());

      m.setObject(testObj);

      return m;
   }

   @Override
   protected void assertEquivalent(final Message m, final int mode, final boolean redelivery) throws JMSException
   {
      super.assertEquivalent(m, mode, redelivery);

      ObjectMessage obj = (ObjectMessage)m;

      ProxyAssertSupport.assertNotNull(obj.getObject());
      ProxyAssertSupport.assertEquals(obj.getObject(), testObj);
   }

}
