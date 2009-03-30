/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.jms.bridge;

/**
 * <h3>Quality of server (QoS) levels</h3>
 * 
 * <h4>QOS_AT_MOST_ONCE</h4>
 * 
 * With this QoS mode messages will reach the destination from the source at
 * most once. The messages are consumed from the source and acknowledged before
 * sending to the destination. Therefore there is a possibility that if failure
 * occurs between removing them from the source and them arriving at the
 * destination they could be lost. Hence delivery will occur at most once. This
 * mode is avilable for both persistent and non persistent messages.
 * 
 * <h4>QOS_DUPLICATES_OK</h4>
 * 
 * With this QoS mode, the messages are consumed from the source and then
 * acknowledged after they have been successfully sent to the destination.
 * Therefore there is a possibility that if failure occurs after sending to the
 * destination but before acknowledging them, they could be sent again when the
 * system recovers. I.e. the destination might receive duplicates after a
 * failure. This mode is available for both persistent and non persistent
 * messages.
 * 
 * <h4>QOS_ONCE_AND_ONLY_ONCE</h4>
 * 
 * This QoS mode ensures messages will reach the destination from the source
 * once and only once. (Sometimes this mode is known as "exactly once"). If both
 * the source and the destination are on the same JBoss Messaging server
 * instance then this can be achieved by sending and acknowledging the messages
 * in the same local transaction. If the source and destination are on different
 * servers this is achieved by enlisting the sending and consuming sessions in a
 * JTA transaction. The JTA transaction is controlled by JBoss Transactions JTA
 * implementation which is a fully recovering transaction manager, thus
 * providing a very high degree of durability. If JTA is required then both
 * supplied connection factories need to be XAConnectionFactory implementations.
 * This mode is only available for persistent messages. This is likely to be the
 * slowest mode since it requires extra persistence for the transaction logging.
 * 
 * Note: For a specific application it may possible to provide once and only
 * once semantics without using the QOS_ONCE_AND_ONLY_ONCE QoS level. This can
 * be done by using the QOS_DUPLICATES_OK mode and then checking for duplicates
 * at the destination and discarding them. Some JMS servers provide automatic
 * duplicate message detection functionality, or this may be possible to
 * implement on the application level by maintaining a cache of received message
 * ids on disk and comparing received messages to them. The cache would only be
 * valid for a certain period of time so this approach is not as watertight as
 * using QOS_ONCE_AND_ONLY_ONCE but may be a good choice depending on your
 * specific application.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public enum QualityOfServiceMode
{
   AT_MOST_ONCE(0), DUPLICATES_OK(1), ONCE_AND_ONLY_ONCE(2);

   private final int value;

   QualityOfServiceMode(int value)
   {
      this.value = value;
   }

   public int intValue()
   {
      return value;
   }

   public static QualityOfServiceMode valueOf(int value)
   {
      if (value == AT_MOST_ONCE.value)
      {
         return AT_MOST_ONCE;
      }
      if (value == DUPLICATES_OK.value)
      {
         return DUPLICATES_OK;
      }
      if (value == ONCE_AND_ONLY_ONCE.value)
      {
         return ONCE_AND_ONLY_ONCE;
      }
      throw new IllegalArgumentException("invalid QualityOfServiceMode value: "
            + value);
   }

}
