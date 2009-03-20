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

package org.jboss.messaging.tests.integration.management;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import javax.naming.Context;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A ManagementTestBase
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public abstract class ManagementTestBase extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MBeanServer mbeanServer;

   // Static --------------------------------------------------------

   protected static void consumeMessages(int expected, ClientSession session, SimpleString queue) throws Exception
   {
      ClientConsumer consumer = null;
      try
      {
         consumer = session.createConsumer(queue);
         ClientMessage m = null;
         for (int i = 0; i < expected; i++)
         {
            m = consumer.receive(500);
            assertNotNull("expected to received " + expected + " messages, got only " + (i + 1), m);
            m.acknowledge();
         }
         session.commit();
         m = consumer.receive(500);
         assertNull("received one more message than expected (" + expected + ")", m);
      }
      finally
      {
         if (consumer != null)
         {
            consumer.close();
         }
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      mbeanServer = MBeanServerFactory.createMBeanServer();
   }

   protected void checkNoResource(ObjectName on)
   {
      assertFalse("unexpected resource for " + on, mbeanServer.isRegistered(on));
   }

   protected void checkResource(ObjectName on)
   {
      assertTrue("no resource for " + on, mbeanServer.isRegistered(on));
   }
   

   protected static void checkNoBinding(Context context, String binding)
   {
      try
      {
         context.lookup(binding);
         fail("there must be no resource to look up for " + binding);
      }
      catch (Exception e)
      {
      }
   }

   protected static  Object checkBinding(Context context, String binding) throws Exception
   {
      Object o = context.lookup(binding);
      assertNotNull(o);
      return o;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
