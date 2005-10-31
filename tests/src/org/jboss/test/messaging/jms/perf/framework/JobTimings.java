/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf.framework;

import java.io.Serializable;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class JobTimings implements Serializable
{
   private static final long serialVersionUID = 7463130719952321761L;

   private long initTime;
   
   private long testTime;
   
   public JobTimings(long initTime, long testTime)
   {
      this.initTime = initTime;
      this.testTime = testTime;
      if (testTime == 0)
      {
         
         System.out.println("test time is zero!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
         
         System.exit(0);
         //throw new IllegalArgumentException("testTime cannot be zero");
         //debug
         
      }
   }

   /**
    * Get the initTime.
    * 
    * @return the initTime.
    */
   public long getInitTime()
   {
      return initTime;
   }

   /**
    * Get the testTime.
    * 
    * @return the testTime.
    */
   public long getTestTime()
   {
      return testTime;
   }
}
