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

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.tests.util.RandomUtil;

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

   private static final Logger log = Logger.getLogger(ManagementHelperTest.class);

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
      Message msg = new ClientMessageImpl(false, ChannelBuffers.dynamicBuffer(1024));
      ManagementHelper.putOperationInvocation(msg, resource, operationName, param, params);

      Object[] parameters = ManagementHelper.retrieveOperationParameters(msg);
      assertEquals(2, parameters.length);
      assertEquals(param, parameters[0]);
      Object parameter_2 = parameters[1];
      log.info("type " + parameter_2);
      assertTrue(parameter_2 instanceof Object[]);
      Object[] retrievedParams = (Object[])parameter_2;
      assertEquals(params.length, retrievedParams.length);
      for (int i = 0; i < retrievedParams.length; i++)
      {
         assertEquals(params[i], retrievedParams[i]);
      }
   }

   public void testParams() throws Exception
   {
      String resource = randomString();
      String operationName = randomString();

      int i = randomInt();
      String s = randomString();
      double d = randomDouble();
      boolean b = randomBoolean();
      long l = randomLong();
      Map<String, Object> map = new HashMap<String, Object>();
      String key1 = randomString();
      int value1 = randomInt();
      String key2 = randomString();
      double value2 = randomDouble();
      String key3 = randomString();
      String value3 = randomString();
      String key4 = randomString();
      boolean value4 = randomBoolean();
      String key5 = randomString();
      long value5 = randomLong();
      map.put(key1, value1);
      map.put(key2, value2);
      map.put(key3, value3);
      map.put(key4, value4);
      map.put(key5, value5);

      Map<String, Object> map2 = new HashMap<String, Object>();
      String key2_1 = randomString();
      int value2_1 = randomInt();
      String key2_2 = randomString();
      double value2_2 = randomDouble();
      String key2_3 = randomString();
      String value2_3 = randomString();
      String key2_4 = randomString();
      boolean value2_4 = randomBoolean();
      String key2_5 = randomString();
      long value2_5 = randomLong();
      map2.put(key2_1, value2_1);
      map2.put(key2_2, value2_2);
      map2.put(key2_3, value2_3);
      map2.put(key2_4, value2_4);
      map2.put(key2_5, value2_5);

      Map<String, Object> map3 = new HashMap<String, Object>();
      String key3_1 = randomString();
      int value3_1 = randomInt();
      String key3_2 = randomString();
      double value3_2 = randomDouble();
      String key3_3 = randomString();
      String value3_3 = randomString();
      String key3_4 = randomString();
      boolean value3_4 = randomBoolean();
      String key3_5 = randomString();
      long value3_5 = randomLong();
      map3.put(key3_1, value3_1);
      map3.put(key3_2, value3_2);
      map3.put(key3_3, value3_3);
      map3.put(key3_4, value3_4);
      map3.put(key3_5, value3_5);

      Map[] maps = new Map[] { map2, map3 };

      String strElem0 = randomString();
      String strElem1 = randomString();
      String strElem2 = randomString();

      String[] strArray = new String[] { strElem0, strElem1, strElem2 };

      Object[] params = new Object[] { i, s, d, b, l, map, strArray, maps };

      Message msg = new ClientMessageImpl(false, ChannelBuffers.dynamicBuffer(1024));
      ManagementHelper.putOperationInvocation(msg, resource, operationName, params);

      Object[] parameters = ManagementHelper.retrieveOperationParameters(msg);

      assertEquals(params.length, parameters.length);

      assertEquals(i, parameters[0]);
      assertEquals(s, parameters[1]);
      assertEquals(d, parameters[2]);
      assertEquals(b, parameters[3]);
      assertEquals(l, parameters[4]);
      Map mapRes = (Map)parameters[5];
      assertEquals(map.size(), mapRes.size());
      assertEquals(value1, mapRes.get(key1));
      assertEquals(value2, mapRes.get(key2));
      assertEquals(value3, mapRes.get(key3));
      assertEquals(value4, mapRes.get(key4));
      assertEquals(value5, mapRes.get(key5));
      
      Object[] strArr2 = (Object[])parameters[6];
      assertEquals(strArray.length, strArr2.length);
      assertEquals(strElem0, strArr2[0]);
      assertEquals(strElem1, strArr2[1]);
      assertEquals(strElem2, strArr2[2]);
      
      Object[] mapArray = (Object[])parameters[7];
      assertEquals(2, mapArray.length);
      Map mapRes2 = (Map)mapArray[0];
      assertEquals(map2.size(), mapRes2.size());
      assertEquals(value2_1, mapRes2.get(key2_1));
      assertEquals(value2_2, mapRes2.get(key2_2));
      assertEquals(value2_3, mapRes2.get(key2_3));
      assertEquals(value2_4, mapRes2.get(key2_4));
      assertEquals(value2_5, mapRes2.get(key2_5));
      
      Map mapRes3 = (Map)mapArray[1];
      assertEquals(map3.size(), mapRes3.size());
      assertEquals(value3_1, mapRes3.get(key3_1));
      assertEquals(value3_2, mapRes3.get(key3_2));
      assertEquals(value3_3, mapRes3.get(key3_3));
      assertEquals(value3_4, mapRes3.get(key3_4));
      assertEquals(value3_5, mapRes3.get(key3_5));



   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
