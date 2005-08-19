/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.server.jmx;

import org.jboss.jms.server.ServerPeer;
import org.jboss.logging.Logger;

/**
 * Implementation of QueueMBean
 *
 * @author     <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Partially ported from JBossMQ version by:
 * 
 * @author     Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author     <a href="hiram.chirino@jboss.org">Hiram Chirino</a>
 * @author     <a href="pra@tim.se">Peter Antman</a>
 * 
 * @version    $Revision$
 */
public class Topic
   extends DestinationMBeanSupport
   implements TopicMBean
{
   
   private static final Logger log = Logger.getLogger(Topic.class);
   
   public String getTopicName()
   {
      return destinationName;
   }

   public void startService() throws Exception
   {
      super.startService();
                 
      ServerPeer sp = (ServerPeer)
         server.getAttribute(serverPeer, "ServerPeer");

      sp.getDestinationManager().createTopic(destinationName, jndiName);
            
      log.info("Topic:" + destinationName + " started");
   }

}
