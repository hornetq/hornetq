/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.util.junit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;



/**
 * A TestSuite that filters tests.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SelectiveTestSuite extends TestSuite
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private List methods;
   private List tests;

   // Constructors --------------------------------------------------

   public SelectiveTestSuite(TestSuite original, List methods)
   {
      tests = new ArrayList();
      this.methods = new ArrayList(methods);
      this.methods.add("warning");

      for(Enumeration e = original.tests(); e.hasMoreElements();)
      {
         TestCase tc = (TestCase)e.nextElement();
         if (methods.contains(tc.getName()))
         {
            tests.add(tc);
         }
      }

      if (tests.isEmpty())
      {
         tests.add(new TestCase("warning")
         {
            protected void runTest()
            {
               fail("The SelectiveTestSuite did not select any test.");
            }
         });
      }
   }


   // TestSuite overrides -------------------------------------------

   public Test testAt(int index)
   {
      return (Test)tests.get(index);
   }

   public int testCount()
   {
      return tests.size();
   }

   public Enumeration tests()
   {
      return Collections.enumeration(tests);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
