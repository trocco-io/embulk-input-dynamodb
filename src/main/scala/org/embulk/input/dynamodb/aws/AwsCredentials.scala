package org.embulk.input.dynamodb.aws

import java.util.Optional

import software.amazon.awssdk.auth.credentials.{
  AnonymousCredentialsProvider,
  AwsBasicCredentials,
  AwsCredentialsProvider,
  AwsSessionCredentials,
  DefaultCredentialsProvider,
  EnvironmentVariableCredentialsProvider,
  ProfileCredentialsProvider,
  StaticCredentialsProvider,
  SystemPropertyCredentialsProvider,
  WebIdentityTokenFileCredentialsProvider,
  ContainerCredentialsProvider,
  InstanceProfileCredentialsProvider
}
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.regions.Region
import org.embulk.config.ConfigException
import org.embulk.util.config.{Config, ConfigDefault, Task => EmbulkTask}
import org.embulk.input.dynamodb.aws.AwsCredentials.Task
import org.embulk.input.dynamodb.logger
import org.embulk.util.config.units.LocalFile
import java.nio.file.Paths

object AwsCredentials {

  trait Task extends EmbulkTask {

    @Config("auth_method")
    @ConfigDefault("\"default\"")
    def getAuthMethod: String

    @Config("access_key_id")
    @ConfigDefault("null")
    def getAccessKeyId: Optional[String]

    @Config("secret_access_key")
    @ConfigDefault("null")
    def getSecretAccessKey: Optional[String]

    @Config("session_token")
    @ConfigDefault("null")
    def getSessionToken: Optional[String]

    @Config("profile_file")
    @ConfigDefault("null")
    def getProfileFile: Optional[LocalFile]

    @Config("profile_name")
    @ConfigDefault("\"default\"")
    def getProfileName: String

    @Config("role_arn")
    @ConfigDefault("null")
    def getRoleArn: Optional[String]

    @Config("role_session_name")
    @ConfigDefault("null")
    def getRoleSessionName: Optional[String]

    @Config("role_external_id")
    @ConfigDefault("null")
    def getRoleExternalId: Optional[String]

    @Config("role_session_duration_seconds")
    @ConfigDefault("null")
    def getRoleSessionDurationSeconds: Optional[Int]

    @Config("scope_down_policy")
    @ConfigDefault("null")
    def getScopeDownPolicy: Optional[String]

    @Config("web_identity_token_file")
    @ConfigDefault("null")
    def getWebIdentityTokenFile: Optional[String]
  }

  def apply(task: Task): AwsCredentials = {
    new AwsCredentials(task)
  }
}

class AwsCredentials(task: Task) {

  def createAwsCredentialsProvider: AwsCredentialsProvider = {
    task.getAuthMethod match {
      case "basic" =>
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(
            getRequiredOption(task.getAccessKeyId, "access_key_id"),
            getRequiredOption(task.getSecretAccessKey, "secret_access_key")
          )
        )

      case "env" =>
        EnvironmentVariableCredentialsProvider.create()

      case "instance" =>
        // NOTE: combination of InstanceProfileCredentialsProvider and ContainerCredentialsProvider
        // In SDK v2, we can use ContainerCredentialsProvider which handles both ECS and EC2
        ContainerCredentialsProvider.builder().build()

      case "profile" =>
        val builder = ProfileCredentialsProvider
          .builder()
          .profileName(task.getProfileName)

        if (task.getProfileFile.isPresent) {
          val profilePath =
            Paths.get(task.getProfileFile.get().getFile.getAbsolutePath)
          builder.profileFile(
            software.amazon.awssdk.profiles.ProfileFile
              .builder()
              .content(profilePath)
              .`type`(
                software.amazon.awssdk.profiles.ProfileFile.Type.CREDENTIALS
              )
              .build()
          )
        }

        builder.build()

      case "properties" =>
        SystemPropertyCredentialsProvider.create()

      case "anonymous" =>
        AnonymousCredentialsProvider.create()

      case "session" =>
        StaticCredentialsProvider.create(
          AwsSessionCredentials.create(
            getRequiredOption(task.getAccessKeyId, "access_key_id"),
            getRequiredOption(task.getSecretAccessKey, "secret_access_key"),
            getRequiredOption(task.getSessionToken, "session_token")
          )
        )

      case "assume_role" =>
        val stsClient = StsClient
          .builder()
          .credentialsProvider(DefaultCredentialsProvider.create())
          .build()

        val assumeRoleRequestBuilder = AssumeRoleRequest
          .builder()
          .roleArn(getRequiredOption(task.getRoleArn, "role_arn"))
          .roleSessionName(
            getRequiredOption(task.getRoleSessionName, "role_session_name")
          )

        task.getRoleExternalId.ifPresent(v =>
          assumeRoleRequestBuilder.externalId(v)
        )
        task.getRoleSessionDurationSeconds.ifPresent(v =>
          assumeRoleRequestBuilder.durationSeconds(v)
        )
        task.getScopeDownPolicy.ifPresent(v =>
          assumeRoleRequestBuilder.policy(v)
        )

        StsAssumeRoleCredentialsProvider
          .builder()
          .stsClient(stsClient)
          .refreshRequest(assumeRoleRequestBuilder.build())
          .build()

      case "web_identity_token" =>
        WebIdentityTokenFileCredentialsProvider
          .builder()
          .roleArn(getRequiredOption(task.getRoleArn, "role_arn"))
          .roleSessionName(
            getRequiredOption(task.getRoleSessionName, "role_session_name")
          )
          .webIdentityTokenFile(
            Paths.get(
              getRequiredOption(
                task.getWebIdentityTokenFile,
                "web_identity_token_file"
              )
            )
          )
          .build()

      case "default" =>
        DefaultCredentialsProvider.create()

      case am =>
        throw new ConfigException(
          s"'$am' is unsupported: `auth_method` must be one of ['basic', 'env', 'instance', 'profile', 'properties', 'anonymous', 'session', 'assume_role', 'default']."
        )
    }
  }

  private def getRequiredOption[A](o: Optional[A], name: String): A = {
    o.orElseThrow(() =>
      new ConfigException(
        s"`$name` must be set when `auth_method` is ${task.getAuthMethod}."
      )
    )
  }

}
