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

package org.hornetq.tests.integration.management;

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomDouble;
import static org.hornetq.tests.util.RandomUtil.randomInt;
import static org.hornetq.tests.util.RandomUtil.randomLong;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.client.impl.ClientSessionImpl;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.Message;

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
      Message msg = new ClientMessageImpl((byte)0, false, 0, 0, (byte)4, 1000);
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

      Message msg = new ClientMessageImpl((byte)0, false, 0, 0, (byte)4, 1000);
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
      assertEquals((long)value1, mapRes.get(key1));
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
      assertEquals((long)value2_1, mapRes2.get(key2_1));
      assertEquals(value2_2, mapRes2.get(key2_2));
      assertEquals(value2_3, mapRes2.get(key2_3));
      assertEquals(value2_4, mapRes2.get(key2_4));
      assertEquals(value2_5, mapRes2.get(key2_5));
      
      Map mapRes3 = (Map)mapArray[1];
      assertEquals(map3.size(), mapRes3.size());
      assertEquals((long)value3_1, mapRes3.get(key3_1));
      assertEquals(value3_2, mapRes3.get(key3_2));
      assertEquals(value3_3, mapRes3.get(key3_3));
      assertEquals(value3_4, mapRes3.get(key3_4));
      assertEquals(value3_5, mapRes3.get(key3_5));
   }
   
   public void testMapWithArrayValues() throws Exception
   {
      String resource = randomString();
      String operationName = randomString();

      Map<String, Object> map = new HashMap<String, Object>();
      String key1 = randomString();
      String[] val1 = new String[] { "a", "b", "c" };
      
      log.info("val1 type is " + val1);
      
      String key2 = randomString();
      Integer[] val2 = new Integer[] { 1, 2, 3, 4, 5 };
      
      log.info("val2 type is " + val2);
      
      map.put(key1, val1);
      map.put(key2, val2);
      
      Object[] params = new Object[] { "hello", map };

      Message msg = new ClientMessageImpl((byte)0, false, 0, 0, (byte)4, 1000);
      ManagementHelper.putOperationInvocation(msg, resource, operationName, params);

      Object[] parameters = ManagementHelper.retrieveOperationParameters(msg);

      assertEquals(params.length, parameters.length);
      
      assertEquals("hello", parameters[0]);
      
      Map map2 = (Map)parameters[1];
      assertEquals(2, map2.size());
      
      Object[] arr1 = (Object[])map2.get(key1);
      assertEquals(val1.length, arr1.length);
      assertEquals(arr1[0], val1[0]);
      assertEquals(arr1[1], val1[1]);
      assertEquals(arr1[2], val1[2]);
      
      Object[] arr2 = (Object[])map2.get(key2);
      assertEquals(val2.length, arr2.length);
      assertEquals(arr2[0], val2[0]);
      assertEquals(arr2[1], val2[1]);
      assertEquals(arr2[2], val2[2]);
      
   }
   
   public void testFromCommaSeparatedKeyValues() throws Exception
   {
      String str = "key1=1, key2=false, key3=2.0, key4=whatever";
      
      Map<String, Object> map = ManagementHelper.fromCommaSeparatedKeyValues(str);
      assertEquals(4, map.size());
      assertTrue(map.containsKey("key1"));
      assertEquals(1L, map.get("key1"));
      
      assertTrue(map.containsKey("key2"));
      assertEquals(false, map.get("key2"));
      
      assertTrue(map.containsKey("key3"));
      assertEquals(2.0, map.get("key3"));
      
      assertTrue(map.containsKey("key4"));
      assertEquals("whatever", map.get("key4"));
   }
   
   public void testFromCommaSeparatedArrayOfCommaSeparatedKeyValuesForSingleItem() throws Exception
   {
      // if there is a single item, no need to enclose it in { }
      String str = "k11=1, k12=false, k13=2.0, k14=whatever ";
      
      Object[] objects = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(str);
      assertEquals(1, objects.length);

      assertTrue(objects[0] instanceof Map<?, ?>);
      Map<String, Object> map = (Map<String, Object>)objects[0];
      assertEquals(4, map.size());
      assertTrue(map.containsKey("k11"));
      assertEquals(1L, map.get("k11"));
      
      assertTrue(map.containsKey("k12"));
      assertEquals(false, map.get("k12"));
      
      assertTrue(map.containsKey("k13"));
      assertEquals(2.0, map.get("k13"));
      
      assertTrue(map.containsKey("k14"));
      assertEquals("whatever", map.get("k14"));
   }

   public void testFromCommaSeparatedArrayOfCommaSeparatedKeyValues() throws Exception
   {
      String str = "{ k11=1, k12=false, k13=2.0, k14=whatever },{ k21=2, k22=true, k23=23.0, k24=foo }";
      
      Object[] objects = ManagementHelper.fromCommaSeparatedArrayOfCommaSeparatedKeyValues(str);
      assertEquals(2, objects.length);

      assertTrue(objects[0] instanceof Map<?, ?>);
      Map<String, Object> map = (Map<String, Object>)objects[0];
      assertEquals(4, map.size());
      assertTrue(map.containsKey("k11"));
      assertEquals(1L, map.get("k11"));
      
      assertTrue(map.containsKey("k12"));
      assertEquals(false, map.get("k12"));
      
      assertTrue(map.containsKey("k13"));
      assertEquals(2.0, map.get("k13"));
      
      assertTrue(map.containsKey("k14"));
      assertEquals("whatever", map.get("k14"));

      assertTrue(objects[1] instanceof Map<?, ?>);
      map = (Map<String, Object>)objects[1];
      assertEquals(4, map.size());
      assertTrue(map.containsKey("k21"));
      assertEquals(2L, map.get("k21"));
      
      assertTrue(map.containsKey("k22"));
      assertEquals(true, map.get("k22"));
      
      assertTrue(map.containsKey("k23"));
      assertEquals(23.0, map.get("k23"));
      
      assertTrue(map.containsKey("k24"));
      assertEquals("foo", map.get("k24"));
}
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
