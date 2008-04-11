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
package org.jboss.test.messaging.tools.junit;

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
				System.exit(FAILURE_EXIT);
         }
			System.exit(SUCCESS_EXIT);
		}
      catch(Exception e)
      {
			System.err.println(e.getMessage());
			System.exit(EXCEPTION_EXIT);
		}
   }

   // Attributes ----------------------------------------------------

   private List methods = new ArrayList();

   // Constructors --------------------------------------------------

   // TestRunner overrides ------------------------------------------

   public Test getTest(String suiteClassName)
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
   private String[] preProcessCommandLine(String[] args)
   {
      List l = new ArrayList();
      for (int i = 0; i < args.length; i++)
      {
         if ("-t".equals(args[i]))
         {
            i++;
            for(StringTokenizer st = new StringTokenizer(args[i], ","); st.hasMoreTokens(); )
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
