package org.hornetq.tests;


import junit.framework.Assert;
import junit.framework.TestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class CoreUnitTestCase extends TestCase
{
   public CoreUnitTestCase()
   {
      super();
   }

   public CoreUnitTestCase(String name)
   {
      super(name);
   }

   public static void assertEqualsByteArrays(final byte[] expected, final byte[] actual)
   {
      // assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         Assert.assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }


   /**
    * Asserts that latch completes within a (rather large interval).
    * <p>
    * Use this instead of just calling {@code latch.await()}. Otherwise your test may hang the whole
    * test run if it fails to count-down the latch.
    * @param latch
    * @throws InterruptedException
    */
   public static void waitForLatch(CountDownLatch latch) throws InterruptedException
   {
      assertTrue("Latch has got to return within a minute", latch.await(1, TimeUnit.MINUTES));
   }
}
