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
package org.jboss.jms.client;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.impl.Cancel;
import org.jboss.jms.client.impl.CancelImpl;
import org.jboss.jms.client.impl.DeliveryInfo;
import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.util.Logger;

/**
 * 
 * A MessageHandler
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MessageHandler
{
   private static final Logger log = Logger.getLogger(MessageHandler.class);
   
   private static final boolean trace = log.isTraceEnabled();
   
   // This is static so it can be called by the asf layer too
   public static void callOnMessage(ClientSession sess,
         MessageListener listener,
         String consumerID,
         boolean isConnectionConsumer,
         JBossMessage m,
         int ackMode,
         int maxDeliveries,
         ClientSession connectionConsumerSession,
         boolean shouldAck)
   throws JMSException
   {      
      if (checkExpiredOrReachedMaxdeliveries(m, connectionConsumerSession!=null?connectionConsumerSession:sess, maxDeliveries, shouldAck))
      {
         // Message has been cancelled
         return;
      }

      DeliveryInfo deliveryInfo =
         new DeliveryInfo(m, consumerID, connectionConsumerSession, shouldAck);

      m.incDeliveryCount();

      // If this is the callback-handler for a connection consumer we don't want to acknowledge or
      // add anything to the tx for this session.
      if (!isConnectionConsumer)
      {
         // We need to call preDeliver, deliver the message then call postDeliver - this is because
         // it is legal to call session.recover(), or session.rollback() from within the onMessage()
         // method in which case the last message needs to be delivered so it needs to know about it
         sess.preDeliver(deliveryInfo);
      } 

      try
      {
         if (trace) { log.trace("calling listener's onMessage(" + m + ")"); }

         listener.onMessage(m);

         if (trace) { log.trace("listener's onMessage() finished"); }
      }
      catch (RuntimeException e)
      {
         log.error("RuntimeException was thrown from onMessage, " + m.getJMSMessageID() + " will be redelivered", e);

         // See JMS 1.1 spec 4.5.2

         if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
         {              
            sess.recover();
         }
      }   

      // If this is the callback-handler for a connection consumer we don't want to acknowledge
      //or add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         if (trace) { log.trace("Calling postDeliver"); }

         sess.postDeliver();

         if (trace) { log.trace("Called postDeliver"); }
      }   
   }
   
   
   public static boolean checkExpiredOrReachedMaxdeliveries(JBossMessage jbm,
         ClientSession del,
         int maxDeliveries, boolean shouldCancel)
   {
      Message msg = jbm.getCoreMessage();

      boolean expired = msg.isExpired();

      boolean reachedMaxDeliveries = jbm.getDeliveryCount() == maxDeliveries;

      if (expired || reachedMaxDeliveries)
      {
         if (trace)
         {
            if (expired)
            {
               log.trace(msg + " has expired, cancelling to server");
            }
            else
            {
               log.trace(msg + " has reached maximum delivery number " + maxDeliveries +", cancelling to server");
            }
         }

         if (shouldCancel)
         {           
            final Cancel cancel = new CancelImpl(jbm.getDeliveryId(), jbm.getDeliveryCount(),
                  expired, reachedMaxDeliveries);          
            try
            {
               del.cancelDelivery(cancel);
            }
            catch (JMSException e)
            {
               log.error("Failed to cancel delivery", e);
            }   
         }

         return true;
      }
      else
      {
         return false;
      }
   }


}
