/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDTRANSACTION;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.jms.tx.TransactionRequest;
import org.jboss.messaging.core.remoting.wireformat.SendTransactionMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class SendTransactionMessageCodec extends AbstractPacketCodec<SendTransactionMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static byte[] encodeTransactionRequest(TransactionRequest tr) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      tr.write(new DataOutputStream(baos));
      baos.flush();
      return baos.toByteArray();
   }

   // Constructors --------------------------------------------------

   public SendTransactionMessageCodec()
   {
      super(MSG_SENDTRANSACTION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SendTransactionMessage request, RemotingBuffer out) throws Exception
   {
      byte[] encodedTxReq = encodeTransactionRequest(request.getTransactionRequest());

      int bodyLength = INT_LENGTH + encodedTxReq.length;
      
      out.putInt(bodyLength);
      out.putInt(encodedTxReq.length);
      out.put(encodedTxReq);
   }

   @Override
   protected SendTransactionMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int txReqLength = in.getInt();
      byte[] encodedTxReq = new byte[txReqLength];
      in.get(encodedTxReq);
      TransactionRequest tr = decodeTransactionRequest(encodedTxReq);

      return new SendTransactionMessage(tr);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   private static TransactionRequest decodeTransactionRequest(byte[] b) throws Exception
   {
      TransactionRequest tr = new TransactionRequest();
      ByteArrayInputStream bais = new ByteArrayInputStream(b);
      tr.read(new DataInputStream(bais));
      return tr;
   }

   // Inner classes -------------------------------------------------
}
