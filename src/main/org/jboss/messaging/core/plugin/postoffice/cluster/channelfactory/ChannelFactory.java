package org.jboss.messaging.core.plugin.postoffice.cluster.channelfactory;

import org.jgroups.JChannel;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision:$</tt>
 *          <p/>
 *          $Id$
 */
public interface ChannelFactory
{
   public JChannel createSyncChannel() throws Exception;
   public JChannel createASyncChannel() throws Exception;
}
