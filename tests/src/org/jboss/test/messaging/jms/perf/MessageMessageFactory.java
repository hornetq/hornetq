/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageMessageFactory extends AbstractMessageFactory
{
   public Message getMessage(Session sess, int size) throws JMSException
   {
      Message m = sess.createMessage();
      byte[] bytes = getBytes(size);
      String s = new String(bytes);
      m.setStringProperty("s", s);
      return m;
   }
}
