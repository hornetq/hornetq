package org.jboss.messaging.newcore.cluster;

import java.util.List;

/**
 * 
 * A GroupCoordinator
 * 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface GroupCoordinator
{
   /**
    * Join the cluster with the specified id
    * @param id
    * @return The state
    * @throws Exception
    */
   Object join(int id, StateHandler handler) throws Exception;
   
   void leave(int id) throws Exception;
   
   void sendToAll(ClusterMessage request, boolean sychronous) throws Exception;
   
   List<Integer> getMembers();
   
   void registerHandler(GroupHandler handler);
   
   void unregisterHandler(GroupHandler handler);
      
}
