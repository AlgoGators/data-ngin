from __future__ import absolute_import
import warnings
import wrapt

## Enable the import to enable type hinting / code navigation below
# import pip._vendor.requests.adapters


# pip master has most commands moved into _internal folder


@wrapt.when_imported('requests')
@wrapt.when_imported('pip._vendor.requests')
# Support for pipenv v2022.8.5
@wrapt.when_imported('pipenv.patched.pip._vendor.requests')
# Support for pipenv older than v2022.8.5
@wrapt.when_imported('pipenv.patched.notpip._vendor.requests')
def apply_patches(requests):
    override_ssl_handler(requests.adapters.HTTPAdapter)


def override_ssl_handler(adapter):
    # type: (pip._vendor.requests.adapters.HTTPAdapter) -> None
    
    def init_poolmanager(wrapped, _instance, args, kwargs):
        # type: (pip._vendor.requests.adapters.HTTPAdapter.init_poolmanager, None, list, dict) -> None
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.load_default_certs()
        kwargs['ssl_context'] = ssl_context
        wrapped(*args, **kwargs)

    def cert_verify(wrapped, _instance, args, kwargs):
        # type: (pip._vendor.requests.adapters.HTTPAdapter.cert_verify, None, list, dict) -> None
        wrapped(*args, **kwargs)

        # By default Python requests uses the ca_certs from the certifi module
        # But we want to use the certificate store instead.
        # By clearing the ca_certs variable we force it to fall back on that behaviour (handled in urllib3)
        if "conn" in kwargs:
            conn = kwargs["conn"]
        else:
            conn = args[0]

        # our default ssl_context we created from init_poolmanager() has these attributes set:
        #  - check_hostname == True
        #  - verify_mode == VerifyMode.CERT_REQUIRED
        # this ssl_context gets passed all the way down to the HTTPSConnection object from urllib3
        # and urllib3 wants to set ssl_context.verify_mode depending on the verify parameter 
        # (verify is either a bool where False becomes CERT_NONE; or a string pointing to ca bundle)
        # However, the SSLContext class doesnt allow this while check_hostname is True
        # Therefore, we reset check_hostname in case cert_verify() got called with verify == False
        if "verify" in kwargs:
            verify = kwargs["verify"]
        elif len(args) > 2:
            verify = args[2]
        else:
            # this is not reachable because there is no default for this parameter 
            # but it prepares us in case urllib3 might add a default one day
            verify = True
            warnings.warn(
                "Mandatory verify parameter not given, falling back to True.\n"
                "This may or may not be intended behavior and is probably related to an unsupported version of urllib3\n"
                "Please report an issue at https://gitlab.com/alelec/pip-system-certs with your\n"
                "python version included in the description\n"
            )
        if not verify:
            try:
                conn.conn_kw["ssl_context"].check_hostname = False
            except (AttributeError, TypeError, KeyError):
                warnings.warn(
                    "Failed to patch SSL settings for unverified requests (unsupported version of urllib3?)\n"
                    "This may lead to errors when urllib3 tries to modify verify_mode.\n"
                    "Please report an issue at https://gitlab.com/alelec/pip-system-certs with your\n"
                    "python version included in the description\n"
                )

        conn.ca_certs = None

    wrapt.wrap_function_wrapper(adapter, 'init_poolmanager', init_poolmanager)
    wrapt.wrap_function_wrapper(adapter, 'cert_verify', cert_verify)
