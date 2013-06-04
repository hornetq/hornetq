package org.hornetq.rest.util;

import org.hornetq.rest.HornetQRestLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class TimeoutTask implements Runnable
{
   protected boolean running = true;
   protected int interval = 10;
   protected final Lock callbacksLock = new ReentrantLock();
   protected Map<String, Callback> callbacks = new HashMap<String, Callback>();
   protected final Lock pendingCallbacksLock = new ReentrantLock();
   protected Map<String, Callback> pendingCallbacks = new HashMap<String, Callback>();
   protected Thread thread;

   public TimeoutTask(int interval)
   {
      this.interval = interval;
   }

   public interface Callback
   {
      public boolean testTimeout(String token, boolean autoShutdown);
      public void shutdown(String token);
   }

   public synchronized void add(Callback callback, String token)
   {
      if(callbacksLock.tryLock())
      {
         try
         {
            callbacks.put(token, callback);
         }
         finally
         {
            callbacksLock.unlock();
         }
      }
      else
      {
         pendingCallbacksLock.lock();
         try
         {
            pendingCallbacks.put(token, callback);
         }
         finally
         {
            pendingCallbacksLock.unlock();
         }
      }
   }

   public synchronized void remove(String token)
   {
      callbacksLock.lock();
      try
      {
         callbacks.remove(token);
      }
      finally
      {
         callbacksLock.unlock();
      }
   }

   public synchronized void stop()
   {
      running = false;
      thread.interrupt();
   }

   public synchronized int getInterval()
   {
      return interval;
   }

   public synchronized void setInterval(int interval)
   {
      this.interval = interval;
   }

   public void start()
   {
      thread = new Thread(this);
      thread.start();
   }

   public void run()
   {
      while (running)
      {
         try
         {
            Thread.sleep(interval * 1000);
         }
         catch (InterruptedException e)
         {
            running = false;
            break;
         }

         // First, test all known callbacks for timeouts.
         // If the timeout is true, then move it to a separate map.
         Map<String, Callback> expiredCallbacks = new HashMap<String, Callback>();

         int liveConsumers = 0;
         int deadConsumers = 0;

         callbacksLock.lock();
         try
         {
            long startTime = System.currentTimeMillis();
            List<String> tokens = new ArrayList<String>(callbacks.size());
            for (String token : callbacks.keySet())
            {
               tokens.add(token);
            }
            for(String token : tokens)
            {
               Callback callback = callbacks.get(token);
               if(callback.testTimeout(token, false))
               {
                  deadConsumers += 1;
                  expiredCallbacks.put(token, callback);
                  callbacks.remove(token);
               }
               else
               {
                  liveConsumers += 1;
               }
            }
            HornetQRestLogger.LOGGER.debug("Finished testing callbacks for timeouts in " +
                                           (System.currentTimeMillis()-startTime) + "ms. "+
                                           "(Live: " + liveConsumers + ", Expired: " + deadConsumers + ")");

            // Next, move any pending callback additions to the main callbacks map.
            pendingCallbacksLock.lock();
            try
            {
               if(pendingCallbacks.size() > 0)
               {
                  HornetQRestLogger.LOGGER.debug("Found " + pendingCallbacks.size() + " callbacks to add.");
                  callbacks.putAll(pendingCallbacks);
                  pendingCallbacks.clear();
               }
            }
            finally
            {
               pendingCallbacksLock.unlock();
            }
         }
         finally
         {
            callbacksLock.unlock();
         }

         // Finally, freely shutdown all expired consumers.
         if(expiredCallbacks.size() > 0)
         {
            long startTime = System.currentTimeMillis();
            List<String> tokens = new ArrayList<String>(expiredCallbacks.size());
            for(String token : expiredCallbacks.keySet())
            {
               tokens.add(token);
            }
            for(String token : tokens)
            {
               Callback expired = expiredCallbacks.get(token);
               expired.shutdown(token);
            }
         }
      }
   }
}
