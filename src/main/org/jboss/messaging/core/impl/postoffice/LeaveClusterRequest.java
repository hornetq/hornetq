package org.jboss.messaging.core.impl.postoffice;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * A LeaveClusterRequest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1917 $</tt>
 *
 * $Id: LeaveClusterRequest.java 1917 2007-01-08 20:26:12Z clebert.suconic@jboss.com $
 */
class LeaveClusterRequest extends ClusterRequest
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private int nodeId;

   // Constructors ---------------------------------------------------------------------------------

   LeaveClusterRequest(int nodeId)
   {
      this.nodeId = nodeId;
   }

   /**
    * This constructor only exist because it's an Streamable requirement.
    * @see ClusterRequest#createFromStream(java.io.DataInputStream)
    */
   LeaveClusterRequest()
   {
   }

   // Streamable implementation --------------------------------------------------------------------

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(nodeId);
   }

   public void read(DataInputStream in) throws Exception
   {
      nodeId = in.readInt();
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "LeaveClusterRequest[NID="  + nodeId + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   Object execute(RequestTarget office) throws Throwable
   {
      office.handleNodeLeft(nodeId);

      return null;
   }

   byte getType()
   {
      return ClusterRequest.LEAVE_CLUSTER_REQUEST;
   }

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
