package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * 
 * A LeaveClusterRequest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class LeaveClusterRequest extends ClusterRequest
{
   static final int TYPE = 11;

   private int nodeId;

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

   Object execute(PostOfficeInternal office) throws Throwable
   {
      office.handleNodeLeft(nodeId);
      
      return null;
   }

   byte getType()
   {
      return TYPE;
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(nodeId);
   }

   public void read(DataInputStream in) throws Exception
   {
      nodeId = in.readInt();
   }
}
