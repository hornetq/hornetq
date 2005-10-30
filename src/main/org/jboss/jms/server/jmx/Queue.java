/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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
