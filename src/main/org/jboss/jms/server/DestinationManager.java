/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface DestinationManager
{
   public static final String DEFAULT_QUEUE_CONTEXT = "/queue";
   public static final String DEFAULT_TOPIC_CONTEXT = "/topic";

   /**
    * Creates and binds in JNDI a queue. The queue name is unique per JMS provider instance.
    *
    * @param name - the queue name.
    * @param jndiName - the JNDI name to bind the newly created queue to. If null, the queue
    *        will be bound in JNDI under the default context using the specified queue name.
    *
    * @throws Exception
    */
   public void createQueue(String name, String jndiName) throws Exception;

   /**
    * Removes the queue both from JNDI and DestinationManager.
    */
   public void destroyQueue(String name) throws Exception;

   /**
    * Creates and binds in JNDI a topic. The topic name is unique per JMS provider instance.
    *
    * @param name - the topic name.
    * @param jndiName - the JNDI name to bind the newly created topic to. If null, the topic
    *        will be bound in JNDI under the default context using the specified topic name.
    *
    * @throws Exception
    */
   public void createTopic(String name, String jndiName) throws Exception;

   /**
    * Removes the topic both from JNDI and DestinationManager.
    */
   public void destroyTopic(String name) throws Exception;


}
