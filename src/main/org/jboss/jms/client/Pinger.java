/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 */
public class Pinger implements Runnable
{
   private static final Logger log = Logger.getLogger(Pinger.class);

   protected ConnectionDelegate conn;
   
   //TODO Needs to be configurable
   protected static final long PING_INTERVAL = 5000;
   
   protected boolean stopping;
   
   protected Thread pingThread;
   
   public Pinger(ConnectionDelegate conn)
   {
      this.conn = conn;
   }
   
   private synchronized void setStopping()
   {
      stopping = true;
   }
   
   private synchronized boolean isStopping()
   {
      return stopping;
   }
   
   public void start()
   {
      if (log.isTraceEnabled()) { log.trace("Starting pinger"); }
      pingThread = new Thread(this);
      pingThread.setDaemon(true);
      pingThread.start();
      if (log.isTraceEnabled()) { log.trace("Pinger started"); }
   }
   
   public void stop()
   {
      if (log.isTraceEnabled()) { log.trace("Stopping pinger"); }
      setStopping();
      try
      {
         pingThread.interrupt();
         pingThread.join();
      }
      catch (InterruptedException e)
      {
         log.error("Ping thread interrupted");
      }
      if (log.isTraceEnabled()) { log.trace("Pinger stopped"); }
   }
   
   public void run()
   {
      while (!isStopping())
      {
         try
         {
            Thread.sleep(PING_INTERVAL);
         }
         catch (InterruptedException e)
         {
            break;
         }
         if (log.isTraceEnabled()) { log.trace("Pinging server"); }
         if (!isStopping())
         {
            conn.ping();
         }
      }
   }
}
