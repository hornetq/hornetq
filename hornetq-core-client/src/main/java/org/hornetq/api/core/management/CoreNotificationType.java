package org.hornetq.api.core.management;

public enum CoreNotificationType implements NotificationType
{
   BINDING_ADDED(0),
   BINDING_REMOVED(1),
   CONSUMER_CREATED(2),
   CONSUMER_CLOSED(3),
   SECURITY_AUTHENTICATION_VIOLATION(6),
   SECURITY_PERMISSION_VIOLATION(7),
   DISCOVERY_GROUP_STARTED(8),
   DISCOVERY_GROUP_STOPPED(9),
   BROADCAST_GROUP_STARTED(10),
   BROADCAST_GROUP_STOPPED(11),
   BRIDGE_STARTED(12),
   BRIDGE_STOPPED(13),
   CLUSTER_CONNECTION_STARTED(14),
   CLUSTER_CONNECTION_STOPPED(15),
   ACCEPTOR_STARTED(16),
   ACCEPTOR_STOPPED(17),
   PROPOSAL(18),
   PROPOSAL_RESPONSE(19);

   private final int value;

   private CoreNotificationType(final int value)
   {
      this.value = value;
   }

   public int getType()
   {
      return value;
   }

}
