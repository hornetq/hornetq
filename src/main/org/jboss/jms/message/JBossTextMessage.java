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
package org.jboss.jms.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.TextMessage;

/**
 * This class implements javax.jms.TextMessage ported from SpyTextMessage in JBossMQ.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * 
 * @version $Revision$
 *
 * $Id$
 */
public class JBossTextMessage extends JBossMessage implements TextMessage
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 7965361851565655163L;
   
   public static final byte TYPE = 5;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   /**
    * Only deserialization should use this constructor directory
    */
   public JBossTextMessage()
   {     
   }
   
   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossTextMessage(String messageID)
   {
      super(messageID);
   }

   /*
    * This constructor is used to construct messages when retrieved from persistence storage
    */
   public JBossTextMessage(String messageID,
         boolean reliable,
         long expiration,
         long timestamp,
         byte priority,
         int deliveryCount,
         Map coreHeaders,
         Serializable payload,
         String jmsType,
         Object correlationID,
         boolean destinationIsQueue,
         String destination,
         boolean replyToIsQueue,
         String replyTo,
         int connectionID,
         Map jmsProperties)
   {
      super(messageID, reliable, expiration, timestamp, priority, deliveryCount, coreHeaders, payload,
            jmsType, correlationID, destinationIsQueue, destination, replyToIsQueue, replyTo, connectionID,
            jmsProperties);
   }

   /**
    * 
    * Make a shallow copy of another JBossTextMessage
    * 
    * @param other
    */
   public JBossTextMessage(JBossTextMessage other)
   {
      super(other);
   }

   /**
    * A copy constructor for non-JBoss Messaging JMS TextMessages.
    */
   public JBossTextMessage(TextMessage foreign) throws JMSException
   {
      super(foreign);
      String text = foreign.getText();
      if (text != null)
      {
         setText(text);
      }
 
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossTextMessage.TYPE;
   }

   // TextMessage implementation ------------------------------------

   public void setText(String string) throws JMSException
   {
      payload = string;
   }

   public String getText() throws JMSException
   {
      return (String)payload;
   }

   // JBossMessage override -----------------------------------------
   
   public JBossMessage doShallowCopy()
   {
      return new JBossTextMessage(this);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void writePayloadExternal(ObjectOutput out) throws IOException
   {
      if (payload == null)
      {
         out.writeByte(NULL);
      }
      else
      {
         out.write(STRING);
         out.writeUTF((String)payload);
      }
   }

   protected Serializable readPayloadExternal(ObjectInput in)
      throws IOException, ClassNotFoundException
   {
      byte type = in.readByte();
      if (type == NULL)
      {
         return null;
      }
      return in.readUTF();
   }


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   // Public --------------------------------------------------------
}