package org.hornetq.tests.util;

import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.SessionFailureListener;

public final class CountDownSessionFailureListener implements SessionFailureListener
{
   private final CountDownLatch latch;

   public CountDownSessionFailureListener(CountDownLatch latch)
   {
      this.latch = latch;
   }
   @Override
   public void connectionFailed(HornetQException exception, boolean failedOver)
   {
      latch.countDown();
   }

   @Override
   public void beforeReconnect(HornetQException exception)
   {
      // TODO Auto-generated method stub

   }

}
