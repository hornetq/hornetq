package org.hornetq.jms.server.management;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.NotificationType;

public enum JMSNotificationType implements NotificationType
{
   QUEUE_CREATED(0),
   QUEUE_DESTROYED(1),
   TOPIC_CREATED(2),
   TOPIC_DESTROYED(3),
   CONNECTION_FACTORY_CREATED(4),
   CONNECTION_FACTORY_DESTROYED(5);

   public static final SimpleString MESSAGE = new SimpleString("message");

   private int type;
   
   private JMSNotificationType(int type)
   {
      this.type = type;
   }

   @Override
   public int getType()
   {
      return type;
   }

}
