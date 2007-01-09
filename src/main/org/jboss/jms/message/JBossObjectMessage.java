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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.jboss.jms.destination.JBossDestination;

/**
 * This class implements javax.jms.ObjectMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * 
 * @version $Revision$
 *
 * $Id$
 */
public class JBossObjectMessage extends JBossMessage implements ObjectMessage
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -2374448267737763502L;
   
   public static final byte TYPE = 3;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   /**
    * Only deserialization should use this constructor directory
    */
   public JBossObjectMessage()
   {     
   }
   
   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossObjectMessage(long messageID)
   {
      super(messageID);
   }

   /*
    * This constructor is used to construct messages when retrieved from persistence storage
    */
   public JBossObjectMessage(long messageID,
         boolean reliable,
         long expiration,
         long timestamp,
         byte priority,
         Map coreHeaders,
         byte[] payloadAsByteArray,         
         String jmsType,
         String correlationID,
         byte[] correlationIDBytes,
         JBossDestination destination,
         JBossDestination replyTo,
         HashMap jmsProperties)
   {
      super(messageID, reliable, expiration, timestamp, priority, coreHeaders, payloadAsByteArray,
            jmsType, correlationID, correlationIDBytes, destination, replyTo, 
            jmsProperties);
   }


   /**
    * 
    * Make a shallow copy of another JBossObjectMessage
    * @param other
    */
   public JBossObjectMessage(JBossObjectMessage other)
   {
      super(other);
   }

   /**
    * A copy constructor for non-JBoss Messaging JMS ObjectMessages.
    */
   public JBossObjectMessage(ObjectMessage foreign, long id) throws JMSException
   {
      super(foreign, id);

      setObject(foreign.getObject()); 
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossObjectMessage.TYPE;
   }
   

   // ObjectMessage implementation ----------------------------------

   public void setObject(Serializable object) throws JMSException
   {  
      //Store it in it's serialized form
      setPayload(object);
      getPayloadAsByteArray();
      setPayload(null);
   }

   public Serializable getObject() throws JMSException
   {
      return getPayload();     
   }

   // JBossMessage overrides ----------------------------------------

   public JBossMessage doShallowCopy()
   {
      return new JBossObjectMessage(this);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
