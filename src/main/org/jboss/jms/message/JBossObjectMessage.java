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
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;

import org.jboss.logging.Logger;
import org.jboss.util.Classes;

/**
 * This class implements javax.jms.ObjectMessage
 * 
 * It is largely ported from SpyObjectMessage in JBossMQ.
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

   public static final int TYPE = 3;

   // Attributes ----------------------------------------------------

   protected boolean isByteArray = false;

   protected boolean bodyReadOnly = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public JBossObjectMessage()
   {
   }

   public JBossObjectMessage(String messageID,
                             boolean reliable,
                             long expiration,
                             long timestamp,
                             Map coreHeaders,
                             Serializable payload,
                             String jmsType,
                             int priority,
                             Object correlationID,
                             boolean destinationIsQueue,
                             String destination,
                             boolean replyToIsQueue,
                             String replyTo,
                             Map jmsProperties)
   {
      super(messageID, reliable, expiration, timestamp, coreHeaders, payload,
            jmsType, priority, correlationID, destinationIsQueue, destination, replyToIsQueue,
            replyTo, jmsProperties);
   }


   public JBossObjectMessage(JBossObjectMessage other)
   {
      super(other);
      this.bodyReadOnly = other.bodyReadOnly;
      this.isByteArray = other.isByteArray;
      if (other.payload != null)
      {
         this.payload = new byte[((byte[])other.payload).length];
         System.arraycopy((byte[])other.payload, 0, (byte[])this.payload, 0,
                          ((byte[])other.payload).length);
      }
   }

   /**
    * A copy constructor for non-JBoss Messaging JMS ObjectMessages.
    */
   protected JBossObjectMessage(ObjectMessage foreign) throws JMSException
   {
      super(foreign);

      Serializable object = foreign.getObject();
      if (object != null)
      {
         setObject(object);
      }
      
   }

   // Public --------------------------------------------------------

   public int getType()
   {
      return JBossObjectMessage.TYPE;
   }

   // ObjectMessage implementation ----------------------------------

   public void setObject(Serializable object) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("setObject");
      }
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

   public void clearBody() throws JMSException
   {
      payload = null;
      bodyReadOnly = false;
      super.clearBody();
   }
   
   public JBossMessage doClone()
   {
      return new JBossObjectMessage(this);
   }
   
   /** Do any other stuff required to be done after sending the message */
   public void afterSend() throws JMSException
   {      
      super.afterSend();
      
      //Message body must be made read-only
      bodyReadOnly = true;
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      super.writeExternal(out);
      out.writeBoolean(bodyReadOnly);
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

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      super.readExternal(in);
      bodyReadOnly = in.readBoolean();
      isByteArray = in.readBoolean();
      int length = in.readInt();
      if (length < 0)
      {
         payload = null;
      }
      else
      {
         payload = new byte[length];
         in.readFully((byte[])payload);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
