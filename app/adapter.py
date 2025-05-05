#  Copyright 2022 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import json
import sys
from typing import List

import aria.ops.adapter_logging as logging
from aria.ops.adapter_instance import AdapterInstance
from aria.ops.data import Metric
from aria.ops.definition.adapter_definition import AdapterDefinition
from aria.ops.result import CollectResult
from aria.ops.result import EndpointResult
from aria.ops.result import TestResult
from aria.ops.timer import Timer
from constants import ADAPTER_KIND
from constants import ADAPTER_NAME
import requests

logger = logging.getLogger(__name__)


def get_adapter_definition() -> AdapterDefinition:
    """
    The adapter definition defines the object types and attribute types (metric/property) that are present
    in a collection. Setting these object types and attribute types helps VMware Aria Operations to
    validate, process, and display the data correctly.
    :return: AdapterDefinition
    """
    try:
        with Timer(logger, "Get Adapter Definition"):
            definition = AdapterDefinition(ADAPTER_KIND, ADAPTER_NAME)

            # Define the user-editable parameters for this management pack. You typically need to
            # define some kind of credential. In our case, we need a secret API key. Defining it this
            # way assures that it will be stored securely in the internal secret store of VCF Ops.
            credential = definition.define_credential_type("credential", "API Key")
            credential.define_password_parameter("apiKey", "API Secret Key")

            # In addition to the credentials, we need the user to enter the stock ticker they're interested in.
            definition.define_string_parameter(
                "ticker",
                label="Ticker",
                description="Stock ticker symbol",
                required=True,
            )
            # The key 'container_memory_limit' is a special key that is read by the VMware Aria Operations collector to
            # determine how much memory to allocate to the docker container running this adapter. It does not
            # need to be read inside the adapter code.
            definition.define_int_parameter(
                "container_memory_limit",
                label="Adapter Memory Limit (MB)",
                description="Sets the maximum amount of memory VMware Aria Operations can "
                "allocate to the container running this adapter instance.",
                required=True,
                advanced=True,
                default=1024,
            )

            # Define the resource types. In our case, we create a Quote object with the bid and ask price.
            quote = definition.define_object_type("quote", "Quote")
            quote.define_metric("bid", "bid", None)
            quote.define_metric("ask", "ask", None)
            return definition
    except Exception as e:
        logger.error(e)


def test(adapter_instance: AdapterInstance) -> TestResult:
    with Timer(logger, "Test"):
        result = TestResult()
        try:
            # Sample test connection code follows. Replace with your own test connection
            # code. A typical test connection will generally consist of:
            # 1. Read identifier values from adapter_instance that are required to
            #    connect to the target(s)
            # 2. Connect to the target(s), and retrieve some sample data
            # 3. Disconnect cleanly from the target (ensure this happens even if an
            #    error occurs)
            # 4. If any of the above failed, return an error, otherwise pass.

            # Read the 'ID' identifier in the adapter instance and use it for a
            # connection test.
            ticker = adapter_instance.get_identifier_value("ticker")
            api_key = adapter_instance.get_credential_value("apiKey")
            response = requests.get(f"https://api.finage.co.uk/last/stock/{ticker}?apikey={api_key}")
            if response.status_code != 200:
                result.with_error("Error connecting to market")
        except Exception as e:
            logger.error("Unexpected connection test error")
            logger.exception(e)
            result.with_error("Unexpected connection test error: " + repr(e))
        finally:
            # TODO: If any connections are still open, make sure they are closed before returning
            logger.debug(f"Returning test result: {result.get_json()}")
            return result


def collect(adapter_instance: AdapterInstance) -> CollectResult:
    with Timer(logger, "Collection"):
        result = CollectResult()
        try:
            # Sample collection code follows. Replace this with your own collection
            # code. A typical collection will generally consist of:
            # 1. Read identifier values from adapter_instance that are required to
            #    connect to the target(s)
            # 2. Connect to the target(s), and retrieve data
            # 3. Add the data into a CollectResult's objects, properties, metrics, etc
            # 4. Disconnect cleanly from the target (ensure this happens even if an
            #    error occurs)
            # 5. Return the CollectResult.

            # Collect the API key and ticker symbol
            ticker = adapter_instance.get_identifier_value("ticker")
            api_key = adapter_instance.get_credential_value("apiKey")

            # Call the market API
            q = requests.get(f"https://api.finage.co.uk/last/stock/{ticker}?apikey={api_key}").json()

            # Create an instance of the Quote object and give it the name of the ticker. The resource will
            # be automatically created in VCF Ops if it didn't already exist.
            quote = result.object(ADAPTER_KIND, "Quote", ticker)
            bid = Metric("bid", q["bid"])
            ask = Metric("ask", q["ask"])

            # Add the bid and ask price to the resource.
            quote.add_metric(bid)
            quote.add_metric(ask)
        except Exception as e:
            logger.error("Unexpected collection error")
            logger.exception(e)
            result.with_error("Unexpected collection error: " + repr(e))
        finally:
            # TODO: If any connections are still open, make sure they are closed before returning
            logger.debug(f"Returning collection result {result.get_json()}")
            return result


def get_endpoints(adapter_instance: AdapterInstance) -> EndpointResult:
    with Timer(logger, "Get Endpoints"):
        result = EndpointResult()
        # In the case that an SSL Certificate is needed to communicate to the target,
        # add each URL that the adapter uses here. Often this will be derived from a
        # 'host' parameter in the adapter instance. In this Adapter we don't use any
        # HTTPS connections, so we won't add any. If we did, we might do something like
        # this:
        # result.with_endpoint(adapter_instance.get_identifier_value("host"))
        #
        # Multiple endpoints can be returned, like this:
        # result.with_endpoint(adapter_instance.get_identifier_value("primary_host"))
        # result.with_endpoint(adapter_instance.get_identifier_value("secondary_host"))
        #
        # This 'get_endpoints' method will be run before the 'test' method,
        # and VMware Aria Operations will use the results to extract a certificate from
        # each URL. If the certificate is not trusted by the VMware Aria Operations
        # Trust Store, the user will be prompted to either accept or reject the
        # certificate. If it is accepted, the certificate will be added to the
        # AdapterInstance object that is passed to the 'test' and 'collect' methods.
        # Any certificate that is encountered in those methods should then be validated
        # against the certificate(s) in the AdapterInstance.
        logger.debug(f"Returning endpoints: {result.get_json()}")
        return result


# Main entry point of the adapter. You should not need to modify anything below this line.
def main(argv: List[str]) -> None:
    logging.setup_logging("adapter.log")
    # Start a new log file by calling 'rotate'. By default, the last five calls will be
    # retained. If the logs are not manually rotated, the 'setup_logging' call should be
    # invoked with the 'max_size' parameter set to a reasonable value, e.g.,
    # 10_489_760 (10MB).
    logging.rotate()
    logger.info(f"Running adapter code with arguments: {argv}")
    if len(argv) != 3:
        # `inputfile` and `outputfile` are always automatically appended to the
        # argument list by the server
        logger.error("Arguments must be <method> <inputfile> <ouputfile>")
        sys.exit(1)

    method = argv[0]
    try:
        if method == "test":
            test(AdapterInstance.from_input()).send_results()
        elif method == "endpoint_urls":
            get_endpoints(AdapterInstance.from_input()).send_results()
        elif method == "collect":
            collect(AdapterInstance.from_input()).send_results()
        elif method == "adapter_definition":
            result = get_adapter_definition()
            if type(result) is AdapterDefinition:
                result.send_results()
            else:
                logger.info(
                    "get_adapter_definition method did not return an AdapterDefinition"
                )
                sys.exit(1)
        else:
            logger.error(f"Command {method} not found")
            sys.exit(1)
    finally:
        logger.info(Timer.graph())
        sys.exit(0)


if __name__ == "__main__":
    main(sys.argv[1:])
