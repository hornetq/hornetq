/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.server.jmx;


/**
 * Implementation of QueueMBean
 *
 * @author     <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Partially ported from JBossMQ version by:
 * 
 * @author     Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author     <a href="hiram.chirino@jboss.org">Hiram Chirino</a>
 * @author     <a href="pra@tim.se">Peter Antman</a>
 * 
 * @version    $Revision$
 */
public class Queue extends DestinationMBeanSupport
   implements QueueMBean 
{
   
   public String getQueueName()
   {
      return destinationName;
   }
   
   public void setQueueName(String queueName)
   {
      destinationName = queueName;
   }

   public void startService() throws Exception
   {
      super.startService();            
      
      server.invoke(destinationManager, "createQueue",
            new Object[] {destinationName, jndiName},
            new String[] {"java.lang.String", "java.lang.String"}); 
      
      log.info("Queue:" + destinationName + " started");
   }
   
   public void stopService() throws Exception
   {
      server.invoke(destinationManager, "destroyQueue",
            new Object[] {destinationName},
            new String[] {"java.lang.String"});
      log.info("Queue:" + destinationName + " stopped");
   }
   
 }
