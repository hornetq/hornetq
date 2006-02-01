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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;

import org.jboss.logging.Logger;
import org.jboss.util.Classes;

/**
 * This class implements javax.jms.ObjectMessage
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * 
 * @version $Revision$
 *
 * $Id$
 */
public class JBossObjectMessage extends JBossMessage implements ObjectMessage
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -1626960567569667875L;

   private static final Logger log = Logger.getLogger(JBossObjectMessage.class);

   public static final byte TYPE = 3;

   // Attributes ----------------------------------------------------

   protected boolean isByteArray = false;

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
   public JBossObjectMessage(String messageID)
   {
      super(messageID);
   }

   /*
    * This constructor is used to construct messages when retrieved from persistence storage
    */
   public JBossObjectMessage(String messageID,
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
    * Make a shallow copy of another JBossObjectMessage
    * @param other
    */
   public JBossObjectMessage(JBossObjectMessage other)
   {
      super(other);
      this.isByteArray = other.isByteArray;
//      if (other.payload != null)
//      {
//         this.payload = new byte[((byte[])other.payload).length];
//         System.arraycopy((byte[])other.payload, 0, (byte[])this.payload, 0,
//                          ((byte[])other.payload).length);
//      }
   }

   /**
    * A copy constructor for non-JBoss Messaging JMS ObjectMessages.
    */
   public JBossObjectMessage(ObjectMessage foreign) throws JMSException
   {
      super(foreign);

      Serializable object = foreign.getObject();
      if (object != null)
      {
         setObject(object);
      }
      
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossObjectMessage.TYPE;
   }
   

   // ObjectMessage implementation ----------------------------------

   public void setObject(Serializable object) throws JMSException
   {
      if (object == null)
      {
         payload = null;
         return;
      }
      try
      {
         if (object instanceof byte[])
         {
            //cheat for byte arrays
            isByteArray = true;
            payload = new byte[((byte[]) object).length];
            System.arraycopy(object, 0, payload, 0, ((byte[])payload).length);
         }
         else
         {
            isByteArray = false;
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            ObjectOutputStream objectOut = new ObjectOutputStream(byteArray);
            objectOut.writeObject(object);
            payload = byteArray.toByteArray();
            objectOut.close();
         }
      }
      catch (IOException e)
      {
         log.error(e);
         throw new MessageFormatException("Object cannot be serialized:" + e.getMessage());
      }
   }

   public Serializable getObject() throws JMSException
   {

      Serializable retVal = null;
      try
      {
         if (null != payload)
         {
            if (isByteArray)
            {
               retVal = new byte[((byte[])payload).length];
               System.arraycopy(payload, 0, retVal, 0, ((byte[])payload).length);
            }
            else
            {

               /**
                * Default implementation ObjectInputStream does not work well
                * when running an a micro kernal style app-server like JBoss.
                * We need to look for the Class in the context class loader and
                * not in the System classloader.
                * 
                * Would this be done better by using a MarshaedObject??
                */
               class ObjectInputStreamExt extends ObjectInputStream
               {
                  ObjectInputStreamExt(InputStream is) throws IOException
                  {
                     super(is);
                  }

                  protected Class resolveClass(ObjectStreamClass v)
                        throws IOException, ClassNotFoundException
                  {
                     return Classes.loadClass(v.getName());
                  }
               }
               ObjectInputStream input =
                     new ObjectInputStreamExt(new ByteArrayInputStream((byte[])payload));
               retVal = (Serializable) input.readObject();
               input.close();
            }
         }
      }
      catch (ClassNotFoundException e)
      {
         throw new MessageFormatException("ClassNotFoundException: " + e.getMessage());
      }
      catch (IOException e)
      {
         throw new MessageFormatException("IOException: " + e.getMessage());
      }
      return retVal;
   }

   // JBossMessage overrides ----------------------------------------

   public JBossMessage doShallowCopy()
   {
      return new JBossObjectMessage(this);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void writePayloadExternal(ObjectOutput out) throws IOException
   {
      out.writeBoolean(isByteArray);
      if (payload == null)
      {
         out.writeInt(-1);
      }
      else
      {
         out.writeInt(((byte[])payload).length);
         out.write((byte[])payload);
      }
   }

   protected Serializable readPayloadExternal(ObjectInput in)
      throws IOException, ClassNotFoundException
   {
      isByteArray = in.readBoolean();
      int length = in.readInt();
      if (length < 0)
      {
         return null;
      }
      byte[] payload = new byte[length];
      in.readFully((byte[])payload);
      return payload;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
