# kusto-ingest

Ingest logs data to Kusto with `github.com/Azure/azure-kusto-go`.

## Usage - Ingest file

```
$ kusto-ingest file ./testdata/logs.multijson \
    --mappings-file=./testdata/logs.mapping.json \
    --auth-azcli \
    --kusto-endpoint="https://test.kusto.windows.net" \
    --kusto-database="Test" \
    --kusto-table="TestTable"
```

### Authentication - AZCLI

`kusto-ingest` supports using azcli for authentication. This option is helpful for OIDC based pipeline usage.
You can use `--auth-azcli` to enable it.

```
$ kusto-ingest file ./testdata/logs.multijson \
    # ... other options
    --auth-azcli
```

### Authentication - Managed Identity

`kusto-ingest` supports using managed identity for authentication. This option is helpful for Azure services running in Azure.
You can use `--auth-managed-identity-resource-id=<mi-resource-id>` to enable it.

```
$ kusto-ingest file ./testdata/logs.multijson \
    # ... other options
    --auth-managed-identity-resource-id=<mi-resource-id>
```

### Authentication - Service Principal ID and Secret (not recommended)

`kusto-ingest` supports using service principal ID and secret for authentication. This is helpful for existing
pipeline usage with service principal ID and secret. But this is not recommended for new pipeline usage.

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
