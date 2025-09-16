# kusto-ingest

Ingest logs data to Kusto with `github.com/Azure/azure-kusto-go`.


## Usage

### Ingest file

```
$ kusto-ingest file ./testdata/logs.multijson \
    --mappings-file=./testdata/logs.mapping.json \
    --auth-azcli \
    --kusto-endpoint="https://test.kusto.windows.net" \
    --kusto-database="Test" \
    --kusto-table="TestTable"
```

### Management commands

Run Kusto management commands from a file (e.g., create tables, update policies):

```
$ kusto-ingest management ./testdata/commands.kql \
    --auth-azcli \
    --kusto-endpoint="https://test.kusto.windows.net" \
    --kusto-database="Test"
```

Where `./testdata/commands.kql` contains Kusto management commands, e.g.:

```
.create table TestTable (Timestamp: datetime, Message: string)
.alter table TestTable policy update @'{"SoftDeletePeriod": "P365D"}'
```

#### Options for management subcommand

- `--auth-azcli` or other authentication options (see below)
- `--kusto-endpoint` (required)
- `--kusto-database` (required)


### Authentication

#### AZCLI

Use Azure CLI for authentication (helpful for OIDC-based pipeline usage):

```
$ kusto-ingest file ./testdata/logs.multijson \
    # ... other options
    --auth-azcli
```


#### Managed Identity

Use managed identity for authentication (helpful for Azure services running in Azure):

```
$ kusto-ingest file ./testdata/logs.multijson \
    # ... other options
    --auth-managed-identity-resource-id=<mi-resource-id>
```


#### Service Principal ID and Secret (not recommended)

Use service principal ID and secret for authentication (not recommended for new pipelines):

```
$ kusto-ingest file ./testdata/logs.multijson \
    --auth-tenant-id="<tenant-id>" \
    --auth-client-id="<client-id>" \
    --auth-client-secret="<client-secret>" \
    # ... other options
```

## TODO

- [ ] More file formats support
- [ ] CLI piping

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
