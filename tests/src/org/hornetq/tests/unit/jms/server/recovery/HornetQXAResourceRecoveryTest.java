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

package org.hornetq.tests.unit.jms.server.recovery;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.jms.server.recovery.HornetQXAResourceRecovery;

/**
 * A HornetQXAResourceRecoveryTest
 *
 * @author jmesnil
 *
 *
 */
public class HornetQXAResourceRecoveryTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConfigWithOnlyConnectorFactoryClassName() throws Exception
   {
      String config = "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory";
      HornetQXAResourceRecovery.ConfigParser parser = new HornetQXAResourceRecovery.ConfigParser(config);

      Assert.assertEquals(InVMConnectorFactory.class.getName(), parser.getConnectorFactoryClassName());
      Assert.assertEquals(0, parser.getConnectorParameters().size());
      Assert.assertNull(parser.getUsername());
      Assert.assertNull(parser.getPassword());
   }

   public void testConfigWithConnectorFactoryClassNameAndParamsWithoutUserCredentials() throws Exception
   {
      String config = "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory, , , server-id=99";
      HornetQXAResourceRecovery.ConfigParser parser = new HornetQXAResourceRecovery.ConfigParser(config);

      Assert.assertEquals(InVMConnectorFactory.class.getName(), parser.getConnectorFactoryClassName());
      Assert.assertEquals(1, parser.getConnectorParameters().size());
      Assert.assertEquals("99", parser.getConnectorParameters().get(TransportConstants.SERVER_ID_PROP_NAME));
      Assert.assertNull(parser.getUsername());
      Assert.assertNull(parser.getPassword());
   }

   public void testConfigWithConnectorFactoryClassNameAndParamsAndUserCredentials() throws Exception
   {
      String config = "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory, foo, bar, server-id=99, key=val";
      HornetQXAResourceRecovery.ConfigParser parser = new HornetQXAResourceRecovery.ConfigParser(config);

      Assert.assertEquals(InVMConnectorFactory.class.getName(), parser.getConnectorFactoryClassName());
      Assert.assertEquals(2, parser.getConnectorParameters().size());
      Assert.assertEquals("99", parser.getConnectorParameters().get(TransportConstants.SERVER_ID_PROP_NAME));
      Assert.assertEquals("val", parser.getConnectorParameters().get("key"));
      Assert.assertEquals("foo", parser.getUsername());
      Assert.assertEquals("bar", parser.getPassword());
   }

   public void testConfigWithConnectorFactoryClassNameAndUserCredentialsWithoutParams() throws Exception
   {
      String config = "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory, foo, bar";
      HornetQXAResourceRecovery.ConfigParser parser = new HornetQXAResourceRecovery.ConfigParser(config);

      Assert.assertEquals(InVMConnectorFactory.class.getName(), parser.getConnectorFactoryClassName());
      Assert.assertEquals(0, parser.getConnectorParameters().size());
      Assert.assertEquals("foo", parser.getUsername());
      Assert.assertEquals("bar", parser.getPassword());
   }

   public void testEmptyString() throws Exception
   {
      try
      {
         String config = "";
         new HornetQXAResourceRecovery.ConfigParser(config);
         Assert.fail();
      }
      catch (IllegalArgumentException e)
      {
      }
   }

   public void testUserNameWithoutPassword() throws Exception
   {
      try
      {
         String config = "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory, foo";
         new HornetQXAResourceRecovery.ConfigParser(config);
         Assert.fail();
      }
      catch (IllegalArgumentException e)
      {
      }
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
