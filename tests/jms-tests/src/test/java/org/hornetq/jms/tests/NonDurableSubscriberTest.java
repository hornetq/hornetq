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

package org.hornetq.jms.tests;

import javax.jms.InvalidSelectorException;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.naming.InitialContext;

import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 * Non-durable subscriber tests.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class NonDurableSubscriberTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InitialContext ic;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Test introduced as a result of a TCK failure.
    */
   public void testNonDurableSubscriberOnNullTopic() throws Exception
   {
      TopicConnection conn = null;

      try
      {
         conn = JMSTestCase.cf.createTopicConnection();

         TopicSession ts = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

         try
         {
            ts.createSubscriber(null);
            ProxyAssertSupport.fail("this should fail");
         }
         catch (javax.jms.InvalidDestinationException e)
         {
            // OK
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * Test introduced as a result of a TCK failure.
    */
   public void testNonDurableSubscriberInvalidUnsubscribe() throws Exception
   {
      TopicConnection conn = null;

      try
      {
         conn = JMSTestCase.cf.createTopicConnection();
         conn.setClientID("sofiavergara");

         TopicSession ts = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

         try
         {
            ts.unsubscribe("invalid-subscription-name");
            ProxyAssertSupport.fail("this should fail");
         }
         catch (javax.jms.InvalidDestinationException e)
         {
            // OK
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testInvalidSelectorOnSubscription() throws Exception
   {
      TopicConnection c = null;
      try
      {
         c = JMSTestCase.cf.createTopicConnection();
         c.setClientID("something");

         TopicSession s = c.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

         try
         {
            s.createSubscriber(HornetQServerTestCase.topic1, "=TEST 'test'", false);
            ProxyAssertSupport.fail("this should fail");
         }
         catch (InvalidSelectorException e)
         {
            // OK
         }
      }
      finally
      {
         c.close();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
