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
package org.jboss.test.messaging.jms.perf;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class DrainJob extends BaseJob
{
   private static final long serialVersionUID = -4637670167659745878L;

   private static final Logger log = Logger.getLogger(DrainJob.class);
   
   protected static final long RECEIVE_TIMEOUT = 5000;

   protected String subName;
   
   protected String clientID;
   
   public DrainJob(String slaveURL, String serverURL, String destinationName, String subName, String clientID)
   {
      super(slaveURL, serverURL, destinationName);
      this.subName = subName;
      this.clientID = clientID;
   }
   
   public Object getResult()
   {
      return new JobTimings(0, 0);
   }
   
   public void run()
   { 
      Connection conn = null;
      
      int count = 0;
      
      try
      {
 
         super.setup();
         
         log.info("Running drain job ===============");
         
         conn = cf.createConnection();
         
         if (clientID != null)
         {
            try
            {
               conn.setClientID(clientID);
            }
            catch (Exception e)
            {
               //Some providers may provide a connection with client id already set
            }
         }
         
         conn.start();
      
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
             
         MessageConsumer consumer;
         if (subName == null)
         {
         
            consumer = sess.createConsumer(dest);
         }
         else
         {
            consumer = sess.createDurableSubscriber((Topic)dest, subName);
         }
                    
         while (true)
         {
            Message m = consumer.receive(RECEIVE_TIMEOUT);
            //log.info("received message");
            if (m == null)
            {
               break;
            }
            count++;
         }
         
         log.info("Finished running job===================");         
      }
      catch (Exception e)
      {
         log.error("Failed to drain destination", e);
         failed = true;
      }
      finally
      {
         if (conn != null)
         {
            try
            {
               conn.close();
            }
            catch (Exception e)
            {
               log.error("Failed to close connection", e);
               failed = true;
            }
         }
      }
      

   } 
   
      
   public void setSubName(String subName)
   {
      this.subName = subName;
   }
   
   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }

}
