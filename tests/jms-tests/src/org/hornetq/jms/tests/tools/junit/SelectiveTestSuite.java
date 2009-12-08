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

package org.hornetq.jms.tests.tools.junit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import junit.framework.Assert;
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

   private final List methods;

   private final List tests;

   // Constructors --------------------------------------------------

   public SelectiveTestSuite(final TestSuite original, final List methods)
   {
      tests = new ArrayList();
      this.methods = new ArrayList(methods);
      this.methods.add("warning");

      for (Enumeration e = original.tests(); e.hasMoreElements();)
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
            @Override
            protected void runTest()
            {
               Assert.fail("The SelectiveTestSuite did not select any test.");
            }
         });
      }
   }

   // TestSuite overrides -------------------------------------------

   @Override
   public Test testAt(final int index)
   {
      return (Test)tests.get(index);
   }

   @Override
   public int testCount()
   {
      return tests.size();
   }

   @Override
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