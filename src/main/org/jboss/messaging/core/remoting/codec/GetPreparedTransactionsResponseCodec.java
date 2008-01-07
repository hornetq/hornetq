/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETPREPAREDTRANSACTIONS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.jms.tx.MessagingXid;
import org.jboss.messaging.core.remoting.wireformat.GetPreparedTransactionsResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class GetPreparedTransactionsResponseCodec extends
      AbstractPacketCodec<GetPreparedTransactionsResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static byte[] encode(MessagingXid[] xids) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      for (int i = 0; i < xids.length; i++)
      {
         MessagingXid xid = xids[i];
         xid.write(dos);
      }
      return baos.toByteArray();
   }

   // Constructors --------------------------------------------------

   public GetPreparedTransactionsResponseCodec()
   {
      super(RESP_GETPREPAREDTRANSACTIONS);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(GetPreparedTransactionsResponse response,
         RemotingBuffer out) throws Exception
   {

      MessagingXid[] xids = response.getXids();
      
      byte[] encodedXids = encode(xids);

      int bodyLength = INT_LENGTH + INT_LENGTH + encodedXids.length;

      out.putInt(bodyLength);
      out.putInt(xids.length);
      out.putInt(encodedXids.length);
      out.put(encodedXids);
   }

   @Override
   protected GetPreparedTransactionsResponse decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int numOfXids = in.getInt();
      int encodedXidsLength = in.getInt();
      byte[] b = new byte[encodedXidsLength];
      in.get(b);
      MessagingXid[] xids = decode(numOfXids, b);

      return new GetPreparedTransactionsResponse(xids);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private MessagingXid[] decode(int numOfXids, byte[] encodedXids) throws Exception
   {
      MessagingXid[] xids = new MessagingXid[numOfXids];
      ByteArrayInputStream bais = new ByteArrayInputStream(encodedXids);
      DataInputStream dis = new DataInputStream(bais);
      for (int i = 0; i < xids.length; i++)
      {
         MessagingXid xid = new MessagingXid();
         xid.read(dis);
         xids[i] = xid;
      }
      return xids;
   }
   
   // Inner classes -------------------------------------------------
}
