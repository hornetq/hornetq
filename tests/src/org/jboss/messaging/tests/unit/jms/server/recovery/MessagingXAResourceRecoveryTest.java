/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.unit.jms.server.recovery;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.jms.server.recovery.MessagingXAResourceRecovery;

/**
 * A MessagingXAResourceRecoveryTest
 *
 * @author jmesnil
 *
 *
 */
public class MessagingXAResourceRecoveryTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConfigWithOnlyConnectorFactoryClassName() throws Exception
   {
      String config = "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory";
      MessagingXAResourceRecovery.ConfigParser parser = new MessagingXAResourceRecovery.ConfigParser(config);

      assertEquals(InVMConnectorFactory.class.getName(), parser.getConnectorFactoryClassName());
      assertEquals(0, parser.getConnectorParameters().size());
      assertNull(parser.getUsername());
      assertNull(parser.getPassword());
   }

   public void testConfigWithConnectorFactoryClassNameAndParamsWithoutUserCredentials() throws Exception
   {
      String config = "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory, , , jbm.remoting.invm.serverid=99";
      MessagingXAResourceRecovery.ConfigParser parser = new MessagingXAResourceRecovery.ConfigParser(config);

      assertEquals(InVMConnectorFactory.class.getName(), parser.getConnectorFactoryClassName());
      assertEquals(1, parser.getConnectorParameters().size());
      assertEquals("99", parser.getConnectorParameters().get(TransportConstants.SERVER_ID_PROP_NAME));
      assertNull(parser.getUsername());
      assertNull(parser.getPassword());
   }

   public void testConfigWithConnectorFactoryClassNameAndParamsAndUserCredentials() throws Exception
   {
      String config = "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory, foo, bar, jbm.remoting.invm.serverid=99, key=val";
      MessagingXAResourceRecovery.ConfigParser parser = new MessagingXAResourceRecovery.ConfigParser(config);

      assertEquals(InVMConnectorFactory.class.getName(), parser.getConnectorFactoryClassName());
      assertEquals(2, parser.getConnectorParameters().size());
      assertEquals("99", parser.getConnectorParameters().get(TransportConstants.SERVER_ID_PROP_NAME));
      assertEquals("val", parser.getConnectorParameters().get("key"));
      assertEquals("foo", parser.getUsername());
      assertEquals("bar", parser.getPassword());
   }

   public void testConfigWithConnectorFactoryClassNameAndUserCredentialsWithoutParams() throws Exception
   {
      String config = "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory, foo, bar";
      MessagingXAResourceRecovery.ConfigParser parser = new MessagingXAResourceRecovery.ConfigParser(config);

      assertEquals(InVMConnectorFactory.class.getName(), parser.getConnectorFactoryClassName());
      assertEquals(0, parser.getConnectorParameters().size());
      assertEquals("foo", parser.getUsername());
      assertEquals("bar", parser.getPassword());
   }

   public void testEmptyString() throws Exception
   {
      try
      {
         String config = "";
         new MessagingXAResourceRecovery.ConfigParser(config);
         fail();
      }
      catch (IllegalArgumentException e)
      {
      }
   }

   public void testUserNameWithoutPassword() throws Exception
   {
      try
      {
         String config = "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory, foo";
         new MessagingXAResourceRecovery.ConfigParser(config);
         fail();
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
