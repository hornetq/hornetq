package org.jboss.messaging.core.remoting;

/**
 * used for locating the ConnectorRegistry, can be overridden by the user.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface ConnectorRegistryLocator
{
   ConnectorRegistry locate();
}
