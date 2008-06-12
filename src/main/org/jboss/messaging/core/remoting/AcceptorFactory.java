package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.config.Configuration;

import java.util.List;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface AcceptorFactory
{
   List<Acceptor> createAcceptors(Configuration configuration);
}
