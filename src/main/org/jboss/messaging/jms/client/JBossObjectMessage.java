/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.ObjectInputStreamWithClassLoader;

/**
 * This class implements javax.jms.ObjectMessage
 * 
 * Don't used ObjectMessage if you want good performance!
 * 
 * Serialization is slooooow!
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
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

   public JBossObjectMessage( final ClientSession session)
   {
      super(JBossObjectMessage.TYPE, session);
   }
   
   public JBossObjectMessage(final ClientMessage message, ClientSession session)
   {
      super(message, session);
   }

   /**
    * A copy constructor for foreign JMS ObjectMessages.
    */
   public JBossObjectMessage(final ObjectMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, JBossObjectMessage.TYPE, session);

      setObject(foreign.getObject()); 
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossObjectMessage.TYPE;
   }
   
   public void doBeforeSend() throws Exception
   {
      if (object != null)
      {
         ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
         
         ObjectOutputStream oos = new ObjectOutputStream(baos);
         
         oos.writeObject(object);
         
         oos.flush();
         
         byte[] data = baos.toByteArray();
         
         getBody().putInt(data.length);
         getBody().putBytes(data);
      }
      
      super.doBeforeSend();
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
      if (object == null)
      {
         try
         {
            int len = getBody().getInt();
            byte[] data = new byte[len];
            getBody().getBytes(data);
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStreamWithClassLoader(bais);
            object = (Serializable)ois.readObject();
         }
         catch (Exception e)
         {
            JMSException je = new JMSException("Failed to deserialize object");
            je.setLinkedException(e);
         }
      }
      
      return object;
   }

   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      object = null;
   }
   
   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
