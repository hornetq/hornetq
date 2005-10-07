/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.util.Iterator;
import java.util.Map;

public class SteadyStateProcessor implements Processor
{

   double precision = 10;
   
   public void processResults(Test test, Configuration config, Map results, ResultPersistor persistor)
   {
      //Iterator iter = results.entrySet().iterator();
      
      /*
      
      double totalSend = 0;
      double totalReceive = 0;
      
      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();
         
         Job job = (Job)entry.getKey();
         RunData data = (RunData)entry.getValue();
         
         if (job instanceof SenderJob)
         {
            totalSend += data.currentTP;
         }
         else if (job instanceof ReceiverJob)
         {
            totalReceive += data.currentTP;
         }
         else
         {
            throw new IllegalStateException("Invalid job:" + job);
         }
      }
      
      if (totalSend > totalReceive)
      {
         
      }
      else
      {
         
      }
      */
      
   }

}
