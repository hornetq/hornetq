/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.Serializable;

public class JobResult implements Serializable
{
   long messages;
   
   public long time;
   
   JobResult(long messages, long time)
   {
      this.messages = messages;
      this.time = time;
   }
}
