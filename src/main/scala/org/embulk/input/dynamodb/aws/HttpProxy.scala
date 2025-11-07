package org.embulk.input.dynamodb.aws

import java.util.Optional
import java.net.URI

import software.amazon.awssdk.http.apache.ProxyConfiguration
import org.embulk.config.ConfigException
import org.embulk.util.config.{Task => EmbulkTask, Config, ConfigDefault}
import org.embulk.input.dynamodb.aws.HttpProxy.Task

object HttpProxy {

  trait Task extends EmbulkTask {

    @Config("host")
    @ConfigDefault("null")
    def getHost: Optional[String]

    @Config("port")
    @ConfigDefault("null")
    def getPort: Optional[Int]

    @Config("protocol")
    @ConfigDefault("\"https\"")
    def getProtocol: String

    @Config("user")
    @ConfigDefault("null")
    def getUser: Optional[String]

    @Config("password")
    @ConfigDefault("null")
    def getPassword: Optional[String]

  }

  def apply(task: Task): HttpProxy = {
    new HttpProxy(task)
  }

}

class HttpProxy(task: Task) {

  def configureProxyConfiguration(): ProxyConfiguration.Builder = {
    val builder = ProxyConfiguration.builder()

    if (task.getHost.isPresent && task.getPort.isPresent) {
      val proxyEndpoint =
        s"${task.getProtocol}://${task.getHost.get}:${task.getPort.get}"
      builder.endpoint(URI.create(proxyEndpoint))
    }
    else if (task.getHost.isPresent) {
      throw new ConfigException(
        "Both 'host' and 'port' must be set for proxy configuration"
      )
    }

    task.getUser.ifPresent(v => builder.username(v))
    task.getPassword.ifPresent(v => builder.password(v))

    builder
  }
}
