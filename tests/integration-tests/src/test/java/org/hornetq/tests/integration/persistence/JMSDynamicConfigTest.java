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

package org.hornetq.tests.integration.persistence;

import org.junit.Test;

import java.util.ArrayList;

import javax.naming.NamingException;

import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.tests.util.JMSTestBase;

/**
 * A JMSDynamicConfigTest
 *
 * @author clebertsuconic
 *
 *
 */
public class JMSDynamicConfigTest extends JMSTestBase
{

   @Override
   protected boolean usePersistence()
   {
      return true;
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testStart() throws Exception
   {
      ArrayList<String> connectors = new ArrayList<String>();

      connectors.add("invm");

      ConnectionFactoryConfiguration cfg = new ConnectionFactoryConfigurationImpl("tst", false, connectors, "tt");
      jmsServer.createConnectionFactory(true, cfg, "tst");

      assertNotNull(context.lookup("tst"));
      jmsServer.removeConnectionFactoryFromJNDI("tst");

      try
      {
         context.lookup("tst");
         fail("failure expected");
      }
      catch (NamingException excepted)
      {
      }

      jmsServer.stop();

      OperationContextImpl.clearContext();
      jmsServer.start();

      try
      {
         context.lookup("tst");
         fail("failure expected");
      }
      catch (NamingException excepted)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
