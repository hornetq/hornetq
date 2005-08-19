/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.jmx;

/**
 * MBean interface to Topic
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Partially ported from JBossMQ version
 */
public interface TopicMBean extends DestinationMBean
{
   String getTopicName();
   
   /*
   
   TODO - We need to implement the following operations too
   in order to give equivalent functionality as JBossMQ

   int getAllMessageCount();

   int getDurableMessageCount();

   int getNonDurableMessageCount();

   int getAllSubscriptionsCount();

   int getDurableSubscriptionsCount();

   int getNonDurableSubscriptionsCount();

   java.util.List listAllSubscriptions();

   java.util.List listDurableSubscriptions();

   java.util.List listNonDurableSubscriptions();

   java.util.List listMessages(java.lang.String id) throws java.lang.Exception;

   java.util.List listMessages(java.lang.String id, java.lang.String selector) throws java.lang.Exception;

   List listNonDurableMessages(String id, String sub) throws Exception;

   List listNonDurableMessages(String id, String sub, String selector) throws Exception;

   List listDurableMessages(String id, String name) throws Exception;

   List listDurableMessages(String id, String name, String selector) throws Exception;
   
   */
}
