/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

package org.jboss.test.messaging.util;

import junit.framework.TestCase;
import junit.framework.Assert;
import junit.framework.AssertionFailedError;
import org.jboss.logging.Logger;

/**
 * This class will proxy any JUnit assertions and send then to our log outputs.
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class ProxyAssertSupport extends TestCase
{

   // Static ---------------------------------------------------------------------------------------

   private static Logger log = Logger.getLogger(ProxyAssertSupport.class);

   public static void assertTrue(java.lang.String string, boolean b)
   {
      try
      {
         Assert.assertTrue(string,b);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertTrue(boolean b)
   {
      try
      {
         Assert.assertTrue(b);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertFalse(java.lang.String string, boolean b)
   {
      try
      {
         Assert.assertFalse(string,b);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertFalse(boolean b)
   {
      try
      {
         Assert.assertFalse(b);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void fail(java.lang.String string)

   {
      try
      {
         Assert.fail(string);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void fail()
   {
      try
      {
         Assert.fail();
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, java.lang.Object object, java.lang.Object object1)
   {
      try
      {
         Assert.assertEquals(string,object,object1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.Object object, java.lang.Object object1)
   {
      try
      {
         Assert.assertEquals(object,object1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, java.lang.String string1, java.lang.String string2)
   {
      try
      {
         Assert.assertEquals(string,string1,string2);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, java.lang.String string1)
   {
      try
      {
         Assert.assertEquals(string,string1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, double v, double v1, double v2)
   {
      try
      {
         Assert.assertEquals(string,v,v1,v2);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(double v, double v1, double v2)
   {
      try
      {
         Assert.assertEquals(v,v1,v2);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, float v, float v1, float v2)
   {
      try
      {
         Assert.assertEquals(string,v,v1,v2);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(float v, float v1, float v2)
   {
      try
      {
         Assert.assertEquals(v,v1,v2);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, long l, long l1)
   {
      try
      {
         Assert.assertEquals(string,l,l1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(long l, long l1)
   {
      try
      {
         Assert.assertEquals(l,l1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, boolean b, boolean b1)
   {
      try
      {
         Assert.assertEquals(string,b,b1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(boolean b, boolean b1)
   {
      try
      {
         Assert.assertEquals(b,b1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, byte b, byte b1)
   {
      try
      {
         Assert.assertEquals(string,b,b1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(byte b, byte b1)
   {
      try
      {
         Assert.assertEquals(b,b1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, char c, char c1)
   {
      try
      {
         Assert.assertEquals(string,c,c1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(char c, char c1)
   {
      try
      {
         Assert.assertEquals(c,c1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, short i, short i1)
   {
      try
      {
         Assert.assertEquals(string,i,i1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(short i, short i1)
   {
      try
      {
         Assert.assertEquals(i,i1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(java.lang.String string, int i, int i1)
   {
      try
      {
         Assert.assertEquals(string,i,i1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertEquals(int i, int i1)
   {
      try
      {
         Assert.assertEquals(i,i1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertNotNull(java.lang.Object object)
   {
      try
      {
         Assert.assertNotNull(object);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertNotNull(java.lang.String string, java.lang.Object object)
   {
      try
      {
         Assert.assertNotNull(string,object);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertNull(java.lang.Object object)
   {
      try
      {
         Assert.assertNull(object);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertNull(java.lang.String string, java.lang.Object object)
   {
      try
      {
         Assert.assertNull(string,object);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertSame(java.lang.String string, java.lang.Object object, java.lang.Object object1)
   {
      try
      {
         Assert.assertSame(string,object,object1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertSame(java.lang.Object object, java.lang.Object object1)
   {
      try
      {
         Assert.assertSame(object,object1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertNotSame(java.lang.String string, java.lang.Object object, java.lang.Object object1)
   {
      try
      {
         Assert.assertNotSame(string,object,object1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

    public static void assertNotSame(java.lang.Object object, java.lang.Object object1)
   {
      try
      {
         Assert.assertNotSame(object,object1);
      }
      catch (AssertionFailedError e)
      {
         log.warn("AssertionFailure::"+e.toString(),e);
         throw e;
      }
   }

   // Constructors ---------------------------------------------------------------------------------

   public ProxyAssertSupport(String string)
   {
      super(string);
   }

   public ProxyAssertSupport()
   {
      super();

   }


}
