package org.hornetq.tests.unit.ra;

import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;
import java.util.Timer;

public class BootstrapContext implements javax.resource.spi.BootstrapContext
{
   public Timer createTimer() throws UnavailableException
   {
      return null;
   }

   public WorkManager getWorkManager()
   {
      return new WorkManager()
      {
         public void doWork(final Work work) throws WorkException
         {
         }

         public void doWork(final Work work,
                            final long l,
                            final ExecutionContext executionContext,
                            final WorkListener workListener) throws WorkException
         {
         }

         public long startWork(final Work work) throws WorkException
         {
            return 0;
         }

         public long startWork(final Work work,
                               final long l,
                               final ExecutionContext executionContext,
                               final WorkListener workListener) throws WorkException
         {
            return 0;
         }

         public void scheduleWork(final Work work) throws WorkException
         {
            work.run();
         }

         public void scheduleWork(final Work work,
                                  final long l,
                                  final ExecutionContext executionContext,
                                  final WorkListener workListener) throws WorkException
         {
         }
      };
   }

   public XATerminator getXATerminator()
   {
      return null;
   }
}