# Secrets Manager Bootstrap

The CDK imports, but does not create, these stage-scoped secrets:

```text
its4us/<stage>/admin-token
its4us/<stage>/client-token
its4us/<stage>/arcadis-api-key
```

Keeping the values out of CDK prevents them from being written to synthesized CloudFormation templates, deployment metadata, or source control.

Create each secret from a terminal prompt that does not echo input:

```bash
read -rs ADMIN_TOKEN
printf '%s' "$ADMIN_TOKEN" | aws secretsmanager create-secret \
  --name its4us/dev/admin-token \
  --secret-string file:///dev/stdin \
  --description 'ITS4US dev API administrator token'
unset ADMIN_TOKEN
```

Repeat for `client-token` and `arcadis-api-key`, and repeat all three for `stage` and `prod`. If a secret already exists, use `put-secret-value` instead of `create-secret`.

The deployment identity must be allowed to reference the secret metadata. Only the authorizer Lambda receives `GetSecretValue` for the two API tokens, and only `retrieve-pmd-arcadis` receives it for the Arcadis key.

After the new code is deployed:

1. Verify the authorizer and PMD retrieval Lambda functions can read their stage secrets.
2. Rotate the exposed old tokens and Arcadis key at their providers.
3. Review and purge historical CloudWatch, pipeline, EventBridge, and S3 logs containing old values.
4. Do not put secret values in `cdk.json`, context, EventBridge payloads, CodePipeline user parameters, API headers, or Lambda environment variables.

For production, use a break-glass or deployment role to populate the secrets; do not put values in shell history or CI variables unless the CI system provides an approved secret store.
