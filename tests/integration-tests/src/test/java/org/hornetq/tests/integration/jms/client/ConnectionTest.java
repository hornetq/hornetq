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
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.hornetq.tests.util.JMSTestBase;
import org.junit.Test;

/**
 * A ConnectionTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
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

   @Test
   public void testConnectionFactorySerialization() throws Exception
   {
      //first try cf without any connection being created
      ConnectionFactory newCF = getCFThruSerialization(cf);
      testCreateConnection(newCF);

      //now serialize a cf after a connection has been created
      //https://issues.jboss.org/browse/WFLY-327
      Connection aConn = null;
      try
      {
         aConn = cf.createConnection();
         newCF = getCFThruSerialization(cf);
         testCreateConnection(newCF);
      }
      finally
      {
         if (aConn != null)
         {
            aConn.close();
         }
      }

   }

   private ConnectionFactory getCFThruSerialization(ConnectionFactory fact) throws Exception
   {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);

      oos.writeObject(cf);
      ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bis);
      ConnectionFactory newCF = (ConnectionFactory)ois.readObject();
      oos.close();
      ois.close();

      return newCF;
   }

   private void testCreateConnection(ConnectionFactory fact) throws Exception
   {
      Connection newConn = null;
      try
      {
         newConn = fact.createConnection();
         newConn.start();
         newConn.stop();
         Session session1 = newConn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         session1.close();
         Session session2 = newConn.createSession(true, Session.SESSION_TRANSACTED);
         session2.close();
      }
      finally
      {
         if (newConn != null)
         {
            newConn.close();
         }
      }
   }
}
