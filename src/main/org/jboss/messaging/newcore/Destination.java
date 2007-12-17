package org.jboss.messaging.newcore;

import org.jboss.messaging.util.Streamable;

/**
 * 
 * A Destination
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Destination extends Streamable
{
   String getType();
   
   String getName();
   
   boolean isTemporary();
}
