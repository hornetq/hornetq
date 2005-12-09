/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.factories;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class MessageMessageFactory extends AbstractMessageFactory
{
   private static final long serialVersionUID = 2067915341101651125L;

   public Message getMessage(Session sess, int size) throws JMSException
   {
      Message m = sess.createMessage();
      byte[] bytes = getBytes(size);
      String s = new String(bytes);
      m.setStringProperty("s", s);
      return m;
   }
}
