/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.tools.junit;

import junit.framework.TestSuite;
import junit.framework.Test;
import junit.framework.TestCase;

import java.util.List;
import java.util.Enumeration;
import java.util.ArrayList;
import java.util.Collections;



/**
 * A TestSuite that filters tests.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
