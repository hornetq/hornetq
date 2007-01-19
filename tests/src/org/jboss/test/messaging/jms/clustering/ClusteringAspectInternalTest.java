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

package org.jboss.test.messaging.jms.clustering;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.jms.client.container.ClusteringAspect;
import java.util.Map;
import java.lang.reflect.Method;

/**
 * This class tests internal methods of ClusteringAspect.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClusteringAspectInternalTest extends MessagingTestCase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClusteringAspectInternalTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testGuessFailoverMap() throws Exception
   {
      Map failoverMapTest = new java.util.HashMap();
      failoverMapTest.put(new Integer(1), new Integer(3));
      failoverMapTest.put(new Integer(3), new Integer(4));
      failoverMapTest.put(new Integer(4), new Integer(1));

      assertEquals(new Integer(1), callGuessFailoverID(failoverMapTest, new Integer(0)));
      assertEquals(new Integer(3), callGuessFailoverID(failoverMapTest, new Integer(2)));
      assertEquals(new Integer(1), callGuessFailoverID(failoverMapTest, new Integer(5)));
      assertEquals(new Integer(3), callGuessFailoverID(failoverMapTest, new Integer(1)));

   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   /**
    * guessFailoverID is a private method that I want to test.
    * This method will use reflection to call that method instead of making it public
    * @param map
    * @param value
    */
   private Integer callGuessFailoverID(Map map, Integer value) throws Exception
   {
      Method method = ClusteringAspect.class.getDeclaredMethod("guessFailoverID",
         new Class[]{Map.class, Integer.class});

      method.setAccessible(true);

      return (Integer) method.invoke(null, new Object[]{map, value});
   }

   // Inner classes --------------------------------------------------------------------------------

}
