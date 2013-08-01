/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.InvalidClientIDException;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XASession;

import org.hornetq.tests.util.JMSTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * A ConnectionTest
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ConnectionTest extends JMSTestBase
{

   private Connection conn2;

   @Test
   public void testSetSameIdToDifferentConnections() throws Exception
   {
      String id = "somethingElse" + name.getMethodName();
      conn = cf.createConnection();
      conn2 = cf.createConnection();
      conn.getClientID();
      conn.setClientID(id);
      try
      {
         conn2.setClientID(id);
         Assert.fail("should not happen.");
      }
      catch (InvalidClientIDException expected)
      {
         // expected
      }
   }

   @Test
   public void testGetSetConnectionFactory() throws Exception
   {
      conn = cf.createConnection();

      conn.getClientID();

      conn.setClientID("somethingElse");
   }

   @Test
   public void testTXTypeInvalid() throws Exception
   {
      conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.SESSION_TRANSACTED);

      assertEquals(Session.AUTO_ACKNOWLEDGE, sess.getAcknowledgeMode());

      sess.close();

      TopicSession tpSess = ((TopicConnection)conn).createTopicSession(false, Session.SESSION_TRANSACTED);

      assertEquals(Session.AUTO_ACKNOWLEDGE, tpSess.getAcknowledgeMode());

      tpSess.close();

      QueueSession qSess = ((QueueConnection)conn).createQueueSession(false, Session.SESSION_TRANSACTED);

      assertEquals(Session.AUTO_ACKNOWLEDGE, qSess.getAcknowledgeMode());

      qSess.close();

   }

   @Test
   public void testXAInstanceof() throws Exception
   {
      conn = cf.createConnection();

      assertFalse(conn instanceof XAConnection);
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      assertFalse(sess instanceof XASession);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (conn2 != null)
      {
         conn2.close();
      }
      super.tearDown();
   }
}
