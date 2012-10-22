package org.hornetq.tests.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import junit.framework.TestCase;

/**
 * Test case that hijacks sys-out and sys-err.
 * <p>
 * It is meant to avoid cluttering either during test execution when the tested code (expectedly)
 * writes to these.
 */
public abstract class SilentTestCase extends TestCase
{
   private PrintStream origSysOut;
   private PrintStream origSysErr;

   private PrintStream sysOut;
   private PrintStream sysErr;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      origSysOut = System.out;
      origSysErr = System.err;
      sysOut = new PrintStream(new ByteArrayOutputStream());
      System.setOut(sysOut);
      sysErr = new PrintStream(new ByteArrayOutputStream());
      System.setErr(sysErr);
   }

   @Override
   public void tearDown() throws Exception
   {
      System.setOut(origSysOut);
      System.setErr(origSysErr);
      super.tearDown();
   }
}
