# Event Hubs consumer: connection string via Key Vault + CSI driver (scratch, delete later)

Pseudo, untested.

## Assumptions
- The code and specs are illustrative and untested. Treat them as a starting point, not a drop-in solution.
- An AKS cluster exists with the OIDC issuer and workload identity enabled, and the Secrets Store CSI driver installed (the AKS Key Vault secrets provider add-on).
- A container registry exists that the cluster can pull from, and a pull secret for it already exists in the target namespace.
- An Event Hubs namespace exists on the Standard tier or higher (Basic has no Kafka endpoint), with an event hub to consume from.
- A Key Vault exists that uses the "Azure role-based access control" permission model.
- The consumer is a single-replica Kafka client using the Event Hubs Kafka endpoint (port 9093) and the `$Default` consumer group.
- You can create managed identities, federated credentials and role assignments in the subscription, and have `kubectl` and Docker access.
- The managed identity is created in the same subscription and region as the vault, and the Azure steps are done in the portal.

Files:
- `consumer_connstring.py`: the consumer using the connection string.
- `consumer_workload_identity.py`: the same consumer using workload identity (step 7).
- `k8s-workload-identity.yaml`: the Deployment after the migration (step 7).
- `Dockerfile`: image with the script.
- `secretproviderclass-connstring.yaml`: what to fetch from Key Vault and which Kubernetes secret to create.
- `deployment-connstring.yaml`: the pod that mounts it and runs the script.
- `serviceaccount-connstring.yaml`: the service account the driver uses to log in to Key Vault.

## 1. Get the connection string (Azure portal)
1. Event Hubs namespace (Standard tier or higher; Basic has no Kafka endpoint) > Event Hubs > + Event Hub. The event hub name is the Kafka topic.
2. Namespace (or event hub) > Shared access policies > + Add > tick Listen. Open it and copy "Connection string-primary key".

## 2. Put it in Key Vault (Azure portal)
1. Key Vaults > your vault > Objects > Secrets > Generate/Import.
2. Name: `evh-connection-string`. Value: the connection string from step 1. Create.

## 3. Let the CSI driver read Key Vault with workload identity (Azure portal, one-time)
The driver logs in to Key Vault as a user-assigned managed identity, through the pod's service account. Needs the cluster to have the OIDC issuer and Workload identity enabled (assumed).

The vault's permission model must be "Azure role-based access control" (Key Vault > Settings > Access configuration). If it uses access policies, add a policy with Get on secrets for the identity instead of the role assignment in step 2.

1. Create the identity: Managed Identities > Create (user-assigned), same subscription and region. Copy its Client ID. Copy the tenant ID too (Microsoft Entra ID > Overview).
2. Give it read access: Key Vault > Access control (IAM) > Add > Add role assignment > `Key Vault Secrets User` > Members: Managed identity > select the new identity > Review + assign.
3. Trust the pod's service account: identity > Settings > Federated credentials > Add credential > scenario "Kubernetes accessing Azure resources".
   - Cluster issuer URL: AKS cluster > Overview > OIDC issuer URL
   - Namespace: `<namespace>`
   - Service account name: `kafka-consumer-sa`
   - Name: any label
   Azure then accepts tokens only from that exact namespace and service account.

## 4. Fill in and apply the specs
Replace the `<...>` placeholders (Client ID, tenant ID, key vault name, namespace, registry image, pull secret, event hub names) in the YAML files, then apply in this order:
```bash
kubectl create namespace <namespace>   # skip if it exists; the registry pull secret must exist in it
kubectl apply -f serviceaccount-connstring.yaml
kubectl apply -f secretproviderclass-connstring.yaml
kubectl apply -f deployment-connstring.yaml
```
How it works:
- The service account carries the identity's Client ID in an annotation.
- The pod mounts the CSI volume. When it starts, the driver uses the service account to get an Azure token (this is where the federated credential from step 3 is checked), reads `evh-connection-string` from Key Vault and creates the Kubernetes secret `evh-conn` (from `secretObjects`).
- The env var `EVENTHUB_CONNECTION_STRING` reads `evh-conn`. Nothing in the YAML contains the connection string.
- The secret is created only while a pod mounts the volume, which is why the Deployment has `volumes` and `volumeMounts` even though the app does not read the files.

## 5. Build and push the image to the registry
```bash
docker build -t <registry>/<repo>/kafka-consumer:1 .
docker push <registry>/<repo>/kafka-consumer:1
```

## 6. Check it works
```bash
kubectl -n <namespace> get secret evh-conn
kubectl -n <namespace> logs deploy/kafka-consumer
```
If the pod is stuck in `ContainerCreating`: `kubectl -n <namespace> describe pod <pod>`. The usual causes are a missing role on the vault, a missing federated credential, a service account name or namespace that does not match it, or a wrong `clientID`/`keyvaultName`/`tenantId`.

## 7. Migrate: read Event Hubs with workload identity (no connection string)
Goal: the app gets an Azure token for the managed identity from step 3 and uses it as the Kafka password. The connection string, the Key Vault secret, the CSI volume and the env var are no longer needed by the pod. Four changes: Azure role, pod spec, code, image.

Before you start: the connection-string version (steps 1 to 6) is running, so you can compare and roll back.

### 7.1 Azure: give the identity access to Event Hubs (portal)
1. Event Hubs namespace > Access control (IAM) > Add > Add role assignment.
2. Role: `Azure Event Hubs Data Receiver` (also `Azure Event Hubs Data Sender` if the app publishes).
3. Members: Managed identity > select the identity from step 3 (the same one; it already trusts `kafka-consumer-sa`). Review + assign.
4. Scope: the namespace (as above) or just the one event hub (Event Hub > Access control (IAM)) for less access. Role assignments can take a few minutes to apply.
No new federated credential is needed. The Key Vault role stays as is, because the old version still needs it until you remove it (7.5).

### 7.2 Code: `consumer_workload_identity.py`
Differences from `consumer_connstring.py`:
- `sasl.mechanism` is `OAUTHBEARER` instead of `PLAIN`.
- No `sasl.username` or `sasl.password`, and no `EVENTHUB_CONNECTION_STRING`.
- `oauth_cb` returns an Azure token for the scope `https://<event-hubs-namespace>.servicebus.windows.net/.default`, plus its expiry. It comes from `DefaultAzureCredential`, which finds the identity through the env vars the webhook injects (7.3).
- The Kafka library calls `oauth_cb` again before the token (about 1 hour) expires, so no extra refresh code is needed.
- `azure-identity` is already in the `Dockerfile`.

### 7.3 Pod spec: `k8s-workload-identity.yaml`
Differences from `deployment-connstring.yaml`:
- Label `azure.workload.identity/use: "true"` on the pod. This makes the workload identity webhook add a projected token file and the env vars `AZURE_CLIENT_ID` (from the service account annotation), `AZURE_TENANT_ID`, `AZURE_AUTHORITY_HOST` and `AZURE_FEDERATED_TOKEN_FILE`.
- Same `serviceAccountName: kafka-consumer-sa` (it carries the identity's Client ID).
- No `EVENTHUB_CONNECTION_STRING` env var, no `secretKeyRef`, no CSI `volumes` and `volumeMounts`.
- `command` runs `consumer_workload_identity.py` and the image tag is `:2`.

### 7.4 Build, deploy and verify
```bash
docker build -t <registry>/<repo>/kafka-consumer:2 .
docker push <registry>/<repo>/kafka-consumer:2
kubectl apply -f k8s-workload-identity.yaml     # replaces the connection-string Deployment of the same name
kubectl -n <namespace> logs deploy/kafka-consumer
kubectl -n <namespace> exec deploy/kafka-consumer -- env | grep -E "AZURE_|EVENTHUB"
```
Check, in this order:
1. Startup: the consumer joins the group and receives messages (log lines with topic, partition, offset). If it fails with an authorization error, the role in 7.1 is missing or not applied yet.
2. The env shows `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE` and no `EVENTHUB_CONNECTION_STRING`.
3. About 90 minutes later, without restarting, it is still receiving messages. This is the only check that catches a token refresh problem, since the first token lasts about an hour.

Rollback: `kubectl apply -f deployment-connstring.yaml`. This works as long as the `SecretProviderClass`, the Key Vault secret and the role from step 3 are still in place.

### 7.5 Clean up (after the checks pass)
1. Delete `secretproviderclass-connstring.yaml` from the cluster (`kubectl delete -f secretproviderclass-connstring.yaml`). The secret `evh-conn` disappears with the last pod that used it.
2. Delete the Key Vault secret `evh-connection-string`, and the identity's `Key Vault Secrets User` role if nothing else uses it.
3. Remove the shared access policy from the Event Hubs namespace, or set the namespace's "Local authentication" to disabled, so a connection string can no longer be used to connect. Check first that no other app still uses the namespace's connection strings.
