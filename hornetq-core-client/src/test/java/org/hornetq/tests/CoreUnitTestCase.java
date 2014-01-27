package org.hornetq.tests;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.client.HornetQClientLogger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public abstract class CoreUnitTestCase extends Assert
{
   public CoreUnitTestCase()
   {

   }

   public CoreUnitTestCase(String name)
   {

   }

   public static void assertEqualsByteArrays(final byte[] expected, final byte[] actual)
   {
      // assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         Assert.assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   private static final HornetQClientLogger log = HornetQClientLogger.LOGGER;

   @Rule
   public TestRule watcher = new TestWatcher()
   {
      @Override
      protected void starting(Description description)
      {
         log.info(String.format("#*#*# Starting test: %s()...", description.getMethodName()));
      }

      @Override
      protected void finished(Description description)
      {
         log.info(String.format("#*#*# Finished test: %s()...", description.getMethodName()));
      }
   };

   /**
    * Asserts that latch completes within a (rather large interval).
    * <p/>
    * Use this instead of just calling {@code latch.await()}. Otherwise your test may hang the whole
    * test run if it fails to count-down the latch.
    *
    * @param latch
    * @throws InterruptedException
    */
   public static void waitForLatch(CountDownLatch latch) throws InterruptedException
   {
      assertTrue("Latch has got to return within a minute", latch.await(1, TimeUnit.MINUTES));
   }

   public static int countOccurrencesOf(String str, String sub)
   {
      if (str == null || sub == null || str.length() == 0 || sub.length() == 0)
      {
         return 0;
      }
      int count = 0;
      int pos = 0;
      int idx;
      while ((idx = str.indexOf(sub, pos)) != -1)
      {
         ++count;
         pos = idx + sub.length();
      }
      return count;
   }

}
