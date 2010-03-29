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

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.NamingException;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.tests.util.JMSTestBase;

/**
 * A CreateCFTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class CreateCFTest extends JMSTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected boolean usePersistence()
   {
      return true;
   }

   public void testCreateCF() throws Exception
   {
      ConnectionFactoryConfigurationImpl factCFG = new ConnectionFactoryConfigurationImpl("tst");

      ArrayList<Pair<String, String>> listStr = new ArrayList<Pair<String, String>>();
      listStr.add(new Pair<String, String>("netty", null));

      factCFG.setConnectorNames(listStr);

      jmsServer.createConnectionFactory(factCFG, "/someCF");

      openCon();

      jmsServer.stop();

      jmsServer.start();

      openCon();

      jmsServer.createConnectionFactory(factCFG, "/someCF");


      openCon();


      jmsServer.stop();

   }

   /**
    * @throws NamingException
    * @throws JMSException
    */
   private void openCon() throws NamingException, JMSException
   {
      ConnectionFactory cf = (ConnectionFactory)context.lookup("/someCF");

      Connection conn = cf.createConnection();

      conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      conn.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
