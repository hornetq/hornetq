package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * A LeaveClusterRequest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class LeaveClusterRequest extends ClusterRequest
{
   // Constants ------------------------------------------------------------------------------------

   static final int TYPE = 11;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private int nodeId;

   // Constructors ---------------------------------------------------------------------------------

   public LeaveClusterRequest(int nodeId)
   {
      this.nodeId=nodeId;
   }

   /**
    * This constructor only exist because it's an Streamable requirement.
    * @see ClusterRequest#createFromStream(java.io.DataInputStream)
    */
   public LeaveClusterRequest()
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

   Object execute(PostOfficeInternal office) throws Throwable
   {
      office.handleNodeLeft(nodeId);

      return null;
   }

   byte getType()
   {
      return TYPE;
   }

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
