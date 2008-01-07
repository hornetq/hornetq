/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETTOPOLOGY;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.jms.delegate.TopologyResult;
import org.jboss.messaging.core.remoting.wireformat.GetTopologyResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class GetTopologyResponseCodec extends
      AbstractPacketCodec<GetTopologyResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public byte[] encode(TopologyResult topology) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      topology.write(dos);
      return baos.toByteArray();
   }

   public TopologyResult decodeTopology(byte[] encodedTopology)
         throws Exception
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(encodedTopology);
      DataInputStream dis = new DataInputStream(bais);
      TopologyResult topology = new TopologyResult();
      topology.read(dis);
      return topology;
   }

   // Constructors --------------------------------------------------

   public GetTopologyResponseCodec()
   {
      super(RESP_GETTOPOLOGY);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(GetTopologyResponse response, RemotingBuffer out) throws Exception
   {
      byte[] encodedTopology = encode(response.getTopology());

      int bodyLength = INT_LENGTH + encodedTopology.length;

      out.putInt(bodyLength);
      out.putInt(encodedTopology.length);
      out.put(encodedTopology);
   }

   @Override
   protected GetTopologyResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int encodedTopologyLength = in.getInt();
      byte[] encodedTopology = new byte[encodedTopologyLength];
      in.get(encodedTopology);
      TopologyResult topology = decodeTopology(encodedTopology);

      return new GetTopologyResponse(topology);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
