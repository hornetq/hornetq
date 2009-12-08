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
import java.util.List;
import java.util.StringTokenizer;

import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * A text TestRunner than runs only test methods specified on command line with "-t".
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SelectiveTestRunner extends TestRunner
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   /**
    * Specify -t testName1,testName2,... somewhere in the argument list.
    */
   public static void main(String[] args)
   {
      SelectiveTestRunner runner = new SelectiveTestRunner();

      try
      {
         args = runner.preProcessCommandLine(args);

         TestResult r = runner.start(args);

         if (!r.wasSuccessful())
         {
            System.exit(TestRunner.FAILURE_EXIT);
         }
         System.exit(TestRunner.SUCCESS_EXIT);
      }
      catch (Exception e)
      {
         System.err.println(e.getMessage());
         System.exit(TestRunner.EXCEPTION_EXIT);
      }
   }

   // Attributes ----------------------------------------------------

   private final List methods = new ArrayList();

   // Constructors --------------------------------------------------

   // TestRunner overrides ------------------------------------------

   @Override
   public Test getTest(final String suiteClassName)
   {
      Test t = super.getTest(suiteClassName);
      if (methods.isEmpty())
      {
         return t;
      }
      else
      {
         return new SelectiveTestSuite((TestSuite)t, methods);
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * Scan command line for an argument of type "-t testMethod1,testMethod2,..." and processes it.
    */
   private String[] preProcessCommandLine(final String[] args)
   {
      List l = new ArrayList();
      for (int i = 0; i < args.length; i++)
      {
         if ("-t".equals(args[i]))
         {
            i++;
            for (StringTokenizer st = new StringTokenizer(args[i], ","); st.hasMoreTokens();)
            {
               methods.add(st.nextToken());
            }
         }
         else
         {
            l.add(args[i]);
         }
      }

      String[] a = new String[l.size()];
      return (String[])l.toArray(a);
   }

   // Inner classes -------------------------------------------------
}