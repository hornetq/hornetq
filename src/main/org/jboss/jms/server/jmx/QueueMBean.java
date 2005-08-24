/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.jmx;

/**
 * MBean interface to Queue
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>Partially ported from JBossMQ version
 */
public interface QueueMBean extends DestinationMBean
{ 
   String getQueueName();
   
   
   /*
    
   TODO - We need to implement the following operations too
   in order to give equivalent functionality as JBossMQ
    
   int getQueueDepth() throws java.lang.Exception;

   int getScheduledMessageCount() throws java.lang.Exception;

   int getReceiversCount();

   java.util.List listReceivers();

   java.util.List listMessages() throws java.lang.Exception;

   java.util.List listMessages(java.lang.String selector) throws java.lang.Exception;
   
   */

}
