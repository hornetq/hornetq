package org.hornetq.tests.util;
import org.junit.Before;
import org.junit.After;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Assert;

/**
 * Test case that hijacks sys-out and sys-err.
 * <p>
 * It is meant to avoid cluttering either during test execution when the tested code (expectedly)
 * writes to these.
 */
public abstract class SilentTestCase extends Assert
{
   private PrintStream origSysOut;
   private PrintStream origSysErr;

   private PrintStream sysOut;
   private PrintStream sysErr;

   @Before
   public void setUp() throws Exception
   {

      origSysOut = System.out;
      origSysErr = System.err;
      sysOut = new PrintStream(new ByteArrayOutputStream());
      System.setOut(sysOut);
      sysErr = new PrintStream(new ByteArrayOutputStream());
      System.setErr(sysErr);
   }

   @After
   public void tearDown() throws Exception
   {
      System.setOut(origSysOut);
      System.setErr(origSysErr);

   }
}
