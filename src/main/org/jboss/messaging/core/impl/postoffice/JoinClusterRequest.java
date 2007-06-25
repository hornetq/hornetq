package org.jboss.messaging.core.impl.postoffice;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>22 Jun 2007
 *
 * $Id: $
 *
 */
class JoinClusterRequest extends ClusterRequest
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

	private int nodeId;
	
   private PostOfficeAddressInfo info;

   // Constructors ---------------------------------------------------------------------------------

   JoinClusterRequest(int nodeId, PostOfficeAddressInfo info)
   {
   	this.nodeId = nodeId;
   	
      this.info = info;
   }

   JoinClusterRequest()
   {
   }

   // Streamable implementation --------------------------------------------------------------------

   public void write(DataOutputStream out) throws Exception
   {
   	out.writeInt(nodeId);
   	
      info.write(out);
   }

   public void read(DataInputStream in) throws Exception
   {
   	nodeId = in.readInt();
   	
      info = new PostOfficeAddressInfo();
      
      info.read(in);
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "JoinClusterRequest[info="  + info + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   Object execute(RequestTarget office) throws Throwable
   {
      office.handleNodeJoined(nodeId, info);

      return null;
   }

   byte getType()
   {
      return ClusterRequest.JOIN_CLUSTER_REQUEST;
   }

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
