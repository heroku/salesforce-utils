# Salesforce Utils

This package contains some utility functions for working with the Salesforce Batch (SOAP) API.

## SalesforceBatch

The `SalesforceBatch` class provides a quick way to interact with your salesforce data.

	< set SF_CLIENT vars in your env>
    > from salesforce_utils import SalesforceBatch
    > sf = SalesforceBatch(username=?, password=?)
    > sf

## data_loader

The `salesforce_utils.data.data_loader` module implements a script for loading random data
into a Salesforce instance.

## Salesforce Oauth Request

	<make sure SF_CLIENT vars are set in your env>
    import salesforce_oauth_request

    login = salesforce_oauth_request.login(username=??, password=??)
