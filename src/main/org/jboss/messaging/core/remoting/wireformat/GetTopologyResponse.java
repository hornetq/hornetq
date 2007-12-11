/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETTOPOLOGY;

import org.jboss.jms.delegate.TopologyResult;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class GetTopologyResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final TopologyResult topology;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public GetTopologyResponse(TopologyResult topology)
   {
      super(RESP_GETTOPOLOGY);

      assert topology != null;

      this.topology = topology;
   }

   // Public --------------------------------------------------------

   public TopologyResult getTopology()
   {
      return topology;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", topology=" + topology + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
