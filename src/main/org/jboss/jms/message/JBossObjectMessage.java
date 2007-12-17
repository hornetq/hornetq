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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

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
   
   public JBossObjectMessage(org.jboss.messaging.newcore.Message message, long deliveryID, int deliveryCount)
   {
      super(message, deliveryID, deliveryCount);
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
      beforeReceive();
   }
   
   
   // ObjectMessage implementation ----------------------------------

   public void setObject(Serializable object) throws JMSException
   {  
      checkWrite();
      
      this.object = object;
   }

   public Serializable getObject() throws JMSException
   {
      return object;
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
      ObjectOutputStream oos = new ObjectOutputStream(daos);               
      oos.writeObject(object);
      oos.flush();
   }
   
   protected void readPayload(DataInputStream dais) throws Exception
   {
      ObjectInputStream ois = new ObjectInputStream(dais);
      object = (Serializable)ois.readObject();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
