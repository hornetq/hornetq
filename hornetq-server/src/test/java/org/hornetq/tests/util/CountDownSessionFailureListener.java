package org.hornetq.tests.util;

import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.SessionFailureListener;

public final class CountDownSessionFailureListener implements SessionFailureListener
{
   private final CountDownLatch latch;
   private final ClientSession session;

   public CountDownSessionFailureListener(ClientSession session)
   {
      this(1, session);
   }

   public CountDownSessionFailureListener(int n, ClientSession session)
   {
      latch = new CountDownLatch(n);
      this.session = session;
   }

   public CountDownSessionFailureListener(CountDownLatch latch, ClientSession session)
   {
      this.latch = latch;
      this.session = session;
   }

   @Override
   public void connectionFailed(HornetQException exception, boolean failedOver)
   {
      latch.countDown();
      session.removeFailureListener(this);

   }

   public CountDownLatch getLatch()
   {
      return latch;
   }

   @Override
   public void beforeReconnect(HornetQException exception)
   {
      // No-op
   }

}
