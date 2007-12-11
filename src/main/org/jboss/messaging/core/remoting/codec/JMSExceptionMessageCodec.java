/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.jms.JMSException;

import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.PacketType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSExceptionMessageCodec extends
      AbstractPacketCodec<JMSExceptionMessage>
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSExceptionMessageCodec()
   {
      super(PacketType.MSG_JMSEXCEPTION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(JMSExceptionMessage message, RemotingBuffer out) throws Exception
   {
      JMSException exception = message.getException();
     
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(exception);

      byte[] encodedException = baos.toByteArray();
      
      int bodyLength = INT_LENGTH + encodedException.length;

      out.putInt(bodyLength);
      out.putInt(encodedException.length);
      out.put(encodedException);
   }

   @Override
   protected JMSExceptionMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      int encodedExceptionLength = in.getInt();
      byte[] b = new byte[encodedExceptionLength];
      in.get(b);
      
      ByteArrayInputStream bais = new ByteArrayInputStream(b);
      ObjectInputStream ois = new ObjectInputStream(bais);
      JMSException exception = (JMSException) ois.readObject();
      
      return new JMSExceptionMessage(exception);
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
