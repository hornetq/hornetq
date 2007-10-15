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
package org.jboss.messaging.core.impl.message;

import java.util.Map;

import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.messaging.core.contract.Message;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>  
 * @version <tt>$Revision: 2284 $</tt>
 * 
 * $Id: MessageFactory.java 2284 2007-02-13 06:47:23Z ovidiu.feodorov@jboss.com $
 */
public class MessageFactory
{
   // Constants ------------------------------------------------------------------------------------
	
   // Static ---------------------------------------------------------------------------------------

   public static Message createMessage(byte type)
   {
      Message m = null;
      
      if (type == JBossMessage.TYPE) //1
      {
         m = new JBossMessage();
      }
      else if (type == JBossObjectMessage.TYPE) //2
      {
         m = new JBossObjectMessage();
      }
      else if (type == JBossTextMessage.TYPE)  //3
      {
         m = new JBossTextMessage();
      }
      else if (type == JBossBytesMessage.TYPE)  //4
      {
         m = new JBossBytesMessage();
      }
      else if (type == JBossMapMessage.TYPE)  //5
      {
         m = new JBossMapMessage();
      }
      else if (type == JBossStreamMessage.TYPE) //6
      {
         m = new JBossStreamMessage();
      }
      else if (type == CoreMessage.TYPE) //127
      {
         m = new CoreMessage();
      }
      else
      {
         throw new IllegalArgumentException("Invalid type " + type);
      }
     
      return m;
   }
   
   /*
    * Create a message from persistent storage
    */
   public static Message createMessage(long messageID,
                                       boolean reliable, 
                                       long expiration, 
                                       long timestamp,
                                       byte priority,
                                       Map headers,
                                       byte[] payload,                                                                                    
                                       byte type)

   {
      Message m = null;
      
      switch (type)
      {
         case JBossMessage.TYPE:
         {
            m = new JBossMessage(messageID, reliable, expiration,
                                 timestamp, priority, headers, payload);
            break;
         }
         case JBossObjectMessage.TYPE:
         {
            m =  new JBossObjectMessage(messageID, reliable, expiration,
                                        timestamp, priority, headers, payload);
            break;
         }
         case JBossTextMessage.TYPE:
         {
            m = new JBossTextMessage(messageID, reliable, expiration,
                                     timestamp, priority, headers, payload);
            break;
         }
         case JBossBytesMessage.TYPE:
         {         	
            m = new JBossBytesMessage(messageID, reliable, expiration,
                                      timestamp, priority, headers, payload);
            break;
         }
         case JBossMapMessage.TYPE:
         {
            m = new JBossMapMessage(messageID, reliable, expiration,
                                    timestamp, priority, headers, payload);
            break;
         }
         case JBossStreamMessage.TYPE:
         {
            m = new JBossStreamMessage(messageID, reliable, expiration,
                                       timestamp, priority, headers, payload);
            break;
         }
         case CoreMessage.TYPE:
         {
            m = new CoreMessage(messageID, reliable, expiration,
                                timestamp, priority, headers, payload);
            break;
         }
         default:
         {
            throw new IllegalArgumentException("Unknown type " + type);       
         }
      }
      
      m.setPersisted(true);

      return m;
   }

   // Attributes -----------------------------------------------------------------------------------
   
   // Constructors ---------------------------------------------------------------------------------
   
   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------   
}

