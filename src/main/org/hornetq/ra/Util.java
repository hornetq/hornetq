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
package org.hornetq.ra;

import java.lang.reflect.Method;
import java.util.*;

import javax.naming.Context;
import javax.transaction.TransactionManager;

import org.hornetq.core.logging.Logger;

/**
 * Various utility functions
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class Util
{
   
   private static final Logger log = Logger.getLogger(Util.class);


   /**
    * Private constructor
    */
   private Util()
   {
   }

   /**
    * Compare two strings.
    * @param me First value
    * @param you Second value
    * @return True if object equals else false. 
    */
   public static boolean compare(final String me, final String you)
   {
      // If both null or intern equals
      if (me == you)
      {
         return true;
      }

      // if me null and you are not
      if (me == null && you != null)
      {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Compare two integers.
    * @param me First value
    * @param you Second value
    * @return True if object equals else false. 
    */
   public static boolean compare(final Integer me, final Integer you)
   {
      // If both null or intern equals
      if (me == you)
      {
         return true;
      }

      // if me null and you are not
      if (me == null && you != null)
      {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Compare two longs.
    * @param me First value
    * @param you Second value
    * @return True if object equals else false. 
    */
   public static boolean compare(final Long me, final Long you)
   {
      // If both null or intern equals
      if (me == you)
      {
         return true;
      }

      // if me null and you are not
      if (me == null && you != null)
      {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Compare two doubles.
    * @param me First value
    * @param you Second value
    * @return True if object equals else false. 
    */
   public static boolean compare(final Double me, final Double you)
   {
      // If both null or intern equals
      if (me == you)
      {
         return true;
      }

      // if me null and you are not
      if (me == null && you != null)
      {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Compare two booleans.
    * @param me First value
    * @param you Second value
    * @return True if object equals else false. 
    */
   public static boolean compare(final Boolean me, final Boolean you)
   {
      // If both null or intern equals
      if (me == you)
      {
         return true;
      }

      // if me null and you are not
      if (me == null && you != null)
      {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Lookup an object in the default initial context
    * @param context The context to use
    * @param name the name to lookup
    * @param clazz the expected type
    * @return the object
    * @throws Exception for any error
    */
   public static Object lookup(final Context context, final String name, final Class clazz) throws Exception
   {
      return context.lookup(name);
   }

   public static Hashtable parseHashtableConfig(final String config)
   {
      Hashtable hashtable = new Hashtable();

      String[] topElements = config.split(",");

      for (String element : topElements)
      {
         String expression[] = element.split("=");

         if (expression.length != 2)
         {
            throw new IllegalArgumentException("Invalid expression " + element + " at " + config);
         }

         hashtable.put(expression[0].trim(), expression[1].trim());
      }

      return hashtable;
   }

   public static List<Map<String, Object>> parseConfig(final String config)
   {
      List<Map<String, Object>> result =new ArrayList<Map<String, Object>>();

      String[] topElements = config.split(",");

      for (String topElement : topElements)
      {
         HashMap<String, Object> map = new HashMap<String, Object>();
         result.add(map);

         String elements[] = topElement.split(";");

         for (String element : elements)
         {
            String expression[] = element.split("=");

            if (expression.length != 2)
            {
               throw new IllegalArgumentException("Invalid expression " + element + " at " + config);
            }

            map.put(expression[0].trim(), expression[1].trim());
         }
      }


      return result;
   }

   public static List<String> parseConnectorConnectorConfig(String config)
   {
      List<String> res = new ArrayList<String>();

      String[] elements = config.split(",");

      for (String element : elements)
      {
         res.add(element.trim());
      }

      return res;
   }
   

   /** The Resource adapter can't depend on any provider's specific library. Because of that we use reflection to locate the
    *  transaction manager during startup. 
    *  
    *  
    *  TODO: https://jira.jboss.org/browse/HORNETQ-417 
    *        We should use a proper SPI instead of reflection
    *        We would need to define a proper SPI package for this.
    *  */
   public static TransactionManager locateTM(final String locatorClass, final String locatorMethod)
   {
      try
      {
         ClassLoader loader = Thread.currentThread().getContextClassLoader();
         Class<?> aClass = loader.loadClass(locatorClass);
         Object o = aClass.newInstance();
         Method m = aClass.getMethod(locatorMethod);
         return (TransactionManager)m.invoke(o);
      }
      catch (Throwable e)
      {
         log.debug(e.getMessage(), e);
         return null;
      }
   }

   
}
