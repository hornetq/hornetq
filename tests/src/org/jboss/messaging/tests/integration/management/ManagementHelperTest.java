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

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.List;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.message.Message;

/**
 * A ManagementHelperTest
 *
 * @author jmesnil
 *
 *
 */
public class ManagementHelperTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testArrayOfStringParameter() throws Exception
   {
      String resource = randomString();
      String operationName = randomString();      
      String param = randomString();
      String[] params = new String[] { randomString(), randomString(), randomString() };
      Message msg = new ClientMessageImpl();
      ManagementHelper.putOperationInvocation(msg, resource, operationName, param, params);
      
      List<Object> parameters = ManagementHelper.retrieveOperationParameters(msg);
      assertEquals(2, parameters.size());
      assertEquals(param, parameters.get(0));
      Object parameter_2 = parameters.get(1);
      assertTrue(parameter_2 instanceof String[]);
      String[] retrievedParams = (String[])parameter_2;
      for (int i = 0; i < retrievedParams.length; i++)
      {
         assertEquals(params[i], retrievedParams[i]);
      }
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
