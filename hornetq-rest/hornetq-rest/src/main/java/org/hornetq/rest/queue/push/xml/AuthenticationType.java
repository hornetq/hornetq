package org.hornetq.rest.queue.push.xml;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlSeeAlso;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
@XmlSeeAlso({BasicAuth.class, DigestAuth.class})
public class AuthenticationType implements Serializable
{
   private static final long serialVersionUID = -4856752055689300045L;
}
