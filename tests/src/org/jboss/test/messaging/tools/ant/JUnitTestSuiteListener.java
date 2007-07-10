/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.ant;

import org.apache.tools.ant.taskdefs.optional.junit.JUnitResultFormatter;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.apache.tools.ant.BuildException;
import org.jboss.test.messaging.tools.ServerManagement;

import java.io.OutputStream;
import java.util.List;
import java.util.Iterator;

import junit.framework.Test;
import junit.framework.AssertionFailedError;

/**
 * This class is a hack.
 *
 * I needed a way to intercept the end of a forked ant JUnit test run, in order to perform some
 * clean-up, and this is it: register this class as a JUnit batchtest formatter, and it will get
 * notified on a endTestSuite() event. Very important, it is run in the same address space as the
 * tests themselves.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class JUnitTestSuiteListener implements JUnitResultFormatter
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // JUnitResultFormatter implementation ---------------------------

   public void endTestSuite(JUnitTest suite) throws BuildException
   {
      try
      {
         List destroyed = ServerManagement.destroySpawnedServers();
         if (destroyed.size() > 0)
         {
            StringBuffer sb = new StringBuffer("Destroyed spawned test servers ");
            for(Iterator i = destroyed.iterator(); i.hasNext();)
            {
               sb.append(i.next());
               if (i.hasNext())
               {
                  sb.append(',');
               }
            }
            System.out.println(sb);
         }
      }
      catch(Throwable t)
      {
         t.printStackTrace();
      }
   }

   public void startTestSuite(JUnitTest suite) throws BuildException
   {
      // noop
   }

   public void setOutput(OutputStream out)
   {
      // noop
   }

   public void setSystemOutput(String out)
   {
      // noop
   }

   public void setSystemError(String err)
   {
      // noop
   }

   // TestListener implementation -----------------------------------

   public void addError(Test test, Throwable t)
   {
      // noop
   }

   public void addFailure(Test test, AssertionFailedError t)
   {
      // noop
   }

   public void endTest(Test test)
   {
      // noop
   }

   public void startTest(Test test)
   {
      // noop
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
