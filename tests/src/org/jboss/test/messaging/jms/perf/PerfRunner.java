/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PerfRunner
{
   private static final Logger log = Logger.getLogger(PerfRunner.class);
   
   public static void main(String[] args)
   {
      log.info("Starting JMS performance test");
      
      if (args.length == 0)
      {
         printUsage();
      }
      else
      {
         String testType = args[0];
         if ("sender".equals(testType))
         {            
            new ProducerTest().run(args);
         }
         else if ("receiver".equals(testType))
         {
           new ConsumerTest().run(args);
         }
         else if ("browser".equals(testType))
         {
           // new BrowserTest().run(args);
         }
      }
   }
   
   private static void printUsage()
   {
      log.info("usage:");
      //TODO
   }
}
