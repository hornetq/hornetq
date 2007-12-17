package org.jboss.messaging.newcore.cluster;

import java.util.List;

/**
 * 
 * A GroupHandler
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface GroupHandler
{
   void membersChanged(List<Integer> newMembers);
   
   void onMessage(int fromNodeId, ClusterMessage message);
}
