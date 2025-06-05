import uuid
from datetime import datetime, timezone
from jwcrypto import jwk, jwt
import requests

def _create_request_jwt_token(
    # Claims parameters
    key_id: str,
    client_id: str,
    audience: str,
    scope: str,
    resource: str,  # This will be aud on the generated token
    # Certificate parameters
    private_key: str
) -> jwt.JWT:
    # Generate timestamps
    issued_at = int(datetime.now(timezone.utc).timestamp())
    expires_at = issued_at + 100
    
    jwt_header = {
    'alg': 'RS256',
    'kid': key_id
    }
    
    jwt_claims = {
    'aud': audience,
    'iss': client_id,
    'scope': scope,
    'iat': issued_at,
    'exp': expires_at,
    'resource': resource,
    'jti': str(uuid.uuid4())
    }
    
    jwt_token = jwt.JWT(
        header=jwt_header,
        claims=jwt_claims
    )
    
    jwt_token.make_signed_token(
        jwk.JWK.from_pem(private_key)
    )
    
    return jwt_token.serialize()

def get_maskinporten_access_token(
    # Claims parameters
    key_id: str,
    client_id: str,
    audience: str,
    scope: str,
    resource: str,
    # Certificate parameters
    private_key: str
) -> str:
    """
    Get access token from Maskinporten
    
    :param key_id: Id of the key assigned to the application in Maskinporten.
    :param client_id: The client ID of the application requesting the access token.
    :param audience: The audience of THIS JWT, which is the maskinporten endpoints. Example: https://test.sky.maskinporten.no
    :param scope: The scope of the access token. This is a space-separated list of scopes that the application is requesting.
    :param resource: The resource for which the access token is requested. This will become the 'aud' claim in the JWT.
    :param private_key: The private key of the application in PEM format. THIS IS NOT THE PRIVATE KEY OF THE VIRKSOMHETSSERTIFIKAT.
    """
    # Create a JWT token
    jwt_token = _create_request_jwt_token(
        key_id=key_id,
        client_id=client_id,
        audience=audience,
        scope=scope,
        resource=resource,
        private_key=private_key,
    )
    # Prepare request
    body = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': jwt_token
    }
    # Send request
    response = requests.post(
        url=audience + '/token',
        data=body,
    )
    # Return the access token
    return response.json().get('access_token')