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

import org.junit.Test;

import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;

import org.hornetq.tests.util.JMSTestBase;

/**
 * A ConnectionTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ConnectionTest extends JMSTestBase
{

   @Test
   public void testGetSetConnectionFactory() throws Exception
   {
      conn = cf.createConnection();

      conn.getClientID();

      conn.setClientID("somethingElse");

   }

   @Test
   public void testXAInstanceof() throws Exception
   {
      conn = cf.createConnection();

      assertFalse(conn instanceof XAConnection);
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      assertFalse(sess instanceof XASession);
   }
}
