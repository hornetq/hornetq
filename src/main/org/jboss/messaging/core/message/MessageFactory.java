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
package org.jboss.messaging.core.message;

import org.jboss.messaging.core.Message;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossStreamMessage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>  
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class MessageFactory
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static Message createMessage(byte type)
   {
      Message m = null;
      
      if (type == JBossMessage.TYPE)
      {
         m = new JBossMessage();
      }
      else if (type == JBossObjectMessage.TYPE)
      {
         m = new JBossObjectMessage();
      }
      else if (type == JBossTextMessage.TYPE)
      {
         m = new JBossTextMessage();
      }
      else if (type == JBossBytesMessage.TYPE)
      {
         m = new JBossBytesMessage();
      }
      else if (type == JBossMapMessage.TYPE)
      {
         m = new JBossMapMessage();
      }
      else if (type == JBossStreamMessage.TYPE)
      {
         m = new JBossStreamMessage();
      }
     
      return m;
   }
   
   public static Message createMessage(Serializable messageID,
                                       boolean reliable, 
                                       long expiration, 
                                       long timestamp,
                                       byte priority,
                                       Map coreHeaders,
                                       byte[] payloadAsByteArray,
                                       byte type,
                                       String jmsType,                                       
                                       String correlationID,
                                       byte[] correlationIDBytes,
                                       JBossDestination destination,
                                       JBossDestination replyTo,                                                                              
                                       HashMap jmsProperties)

   {

      Message m = null;
      
      if (type == JBossMessage.TYPE)
      {
         m = new JBossMessage((String)messageID, reliable, expiration, timestamp, priority, coreHeaders,
                              payloadAsByteArray, jmsType, correlationID, correlationIDBytes,
                              destination, replyTo, jmsProperties);
      }
      else if (type == JBossObjectMessage.TYPE)
      {
         m = new JBossObjectMessage((String)messageID, reliable, expiration, timestamp, priority, coreHeaders,
               payloadAsByteArray, jmsType, correlationID, correlationIDBytes,
               destination, replyTo, jmsProperties);
      }
      else if (type == JBossTextMessage.TYPE)
      {
         m = new JBossTextMessage((String)messageID, reliable, expiration, timestamp, priority, coreHeaders,
               payloadAsByteArray, jmsType, correlationID, correlationIDBytes,
               destination, replyTo, jmsProperties);
      }
      else if (type == JBossBytesMessage.TYPE)
      {
         m = new JBossBytesMessage((String)messageID, reliable, expiration, timestamp, priority, coreHeaders,
               payloadAsByteArray, jmsType, correlationID, correlationIDBytes,
               destination, replyTo, jmsProperties);
      }
      else if (type == JBossMapMessage.TYPE)
      {
         m = new JBossMapMessage((String)messageID, reliable, expiration, timestamp, priority, coreHeaders,
               payloadAsByteArray, jmsType, correlationID, correlationIDBytes,
               destination, replyTo, jmsProperties);
      }
      else if (type == JBossStreamMessage.TYPE)
      {
         m = new JBossStreamMessage((String)messageID, reliable, expiration, timestamp, priority, coreHeaders,
               payloadAsByteArray, jmsType, correlationID, correlationIDBytes,
               destination, replyTo, jmsProperties);
      }
      else
      {
         //Core message
         m = new MessageSupport(messageID, reliable, expiration, timestamp,
               priority, 0, coreHeaders,
               payloadAsByteArray);                           
      }

      return m;

   }
      
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
