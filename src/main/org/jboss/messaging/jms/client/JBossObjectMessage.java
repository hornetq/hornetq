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
package org.jboss.messaging.jms.client;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.util.StreamUtils;

/**
 * This class implements javax.jms.ObjectMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * 
 * @version $Revision: 3412 $
 *
 * $Id: JBossObjectMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class JBossObjectMessage extends JBossMessage implements ObjectMessage
{
   // Constants -----------------------------------------------------

   public static final byte TYPE = 2;

   // Attributes ----------------------------------------------------
   
   private Serializable object;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossObjectMessage()
   {
      super(JBossObjectMessage.TYPE);
   }
   
   public JBossObjectMessage(org.jboss.messaging.core.message.Message message, ClientSession session)
   {
      super(message, session);
   }

   /**
    * A copy constructor for foreign JMS ObjectMessages.
    */
   public JBossObjectMessage(ObjectMessage foreign) throws JMSException
   {
      super(foreign, JBossObjectMessage.TYPE);

      setObject(foreign.getObject()); 
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossObjectMessage.TYPE;
   }
   
   public void doBeforeSend() throws Exception
   {
      beforeSend();
   }
   
   public void doBeforeReceive() throws Exception
   {
   }
   
   
   // ObjectMessage implementation ----------------------------------

   public void setObject(Serializable object) throws JMSException
   {  
      checkWrite();
      
      this.object = object;
   }

   // lazy deserialize the Object the first time the client requests it
   public Serializable getObject() throws JMSException
   {
      if (object != null)
      {
         return object;
      }
      else if (message.getPayload() != null)
      {
         DataInputStream dais = new DataInputStream(new ByteArrayInputStream(message.getPayload()));
         try
         {
            readPayload(dais);
         }
         catch (Exception e)
         {
            RuntimeException e2 = new RuntimeException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }         
         return object;
      } else {
         return null;
      }
   }

   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      object = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void writePayload(DataOutputStream daos) throws Exception
   {
      StreamUtils.writeObject(daos, object, false, true);
   }
   
   protected void readPayload(DataInputStream dais) throws Exception
   {
      object = (Serializable)StreamUtils.readObject(dais, true);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
