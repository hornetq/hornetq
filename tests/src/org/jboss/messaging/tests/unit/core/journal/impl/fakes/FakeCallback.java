package org.jboss.messaging.tests.unit.core.journal.impl.fakes;

import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.journal.IOCallback;

public class FakeCallback implements IOCallback
{
   volatile String msg;
   final CountDownLatch latch;
   
   public FakeCallback(final CountDownLatch latch)
   {
      this.latch = latch;
   }
   
   public FakeCallback()
   {
      this.latch = new CountDownLatch(1);
   }
   
   public void done()
   {
      latch.countDown();
   }

   public void onError(final int errorCode, final String errorMessage)
   {
      latch.countDown();
      this.msg = errorMessage;
   }
   
   public void waitComplete() throws Exception
   {
      latch.await();
   }
   
}

