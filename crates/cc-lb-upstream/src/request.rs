use bytes::Bytes;
use cc_lb_domain::{Principal, Upstream};
use http::{HeaderMap, Method};
use url::Url;

use crate::{DialectError, DialectShapeContext, Signer, SignerError, UpstreamDialect};

/// Request produced by an upstream dialect before credentials are applied.
#[derive(Clone, Debug)]
pub struct ShapedRequest {
    url: Url,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
    _seal: crate::private::Seal,
}

impl ShapedRequest {
    /// Returns the destination URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Replaces the destination URL.
    pub fn set_url(&mut self, url: Url) {
        self.url = url;
    }

    /// Returns the HTTP method.
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// Replaces the HTTP method.
    pub fn set_method(&mut self, method: Method) {
        self.method = method;
    }

    /// Returns the request headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Returns mutable request headers for signer-owned changes.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// Returns the request body.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Replaces the request body.
    pub fn set_body(&mut self, body: Bytes) {
        self.body = body;
    }
}

/// Dialect-facing capability used to construct shaped requests.
#[derive(Debug)]
pub struct ShapedRequestBuilder {
    _seal: crate::private::Seal,
}

impl ShapedRequestBuilder {
    /// Creates a shaped request from dialect-owned parts.
    pub fn shaped_request(
        &mut self,
        url: Url,
        method: Method,
        headers: HeaderMap,
        body: Bytes,
    ) -> ShapedRequest {
        ShapedRequest {
            url,
            method,
            headers,
            body,
            _seal: crate::private::Seal,
        }
    }
}

/// Invokes an upstream dialect with a temporary shaped-request capability.
pub fn shape_request(
    dialect: &dyn UpstreamDialect,
    context: &DialectShapeContext,
    upstream: &Upstream,
    principal: &Principal,
) -> Result<ShapedRequest, DialectError> {
    let mut builder = ShapedRequestBuilder {
        _seal: crate::private::Seal,
    };
    dialect.shape(context, upstream, principal, &mut builder)
}

/// Request after a signer has consumed and sealed a shaped request.
#[derive(Clone, Debug)]
pub struct SignedRequest {
    url: Url,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
    _seal: crate::private::Seal,
}

impl SignedRequest {
    /// Seals an owned shaped request after the signer has applied credentials.
    pub fn from_shaped(shaped: ShapedRequest, _capability: &mut SigningCapability) -> Self {
        Self {
            url: shaped.url,
            method: shaped.method,
            headers: shaped.headers,
            body: shaped.body,
            _seal: crate::private::Seal,
        }
    }

    /// Returns the destination URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Returns the HTTP method.
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// Returns the signed request headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Returns the signed request body.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Consumes the signed request into relay-ready parts.
    pub fn into_parts(self) -> (Url, Method, HeaderMap, Bytes) {
        (self.url, self.method, self.headers, self.body)
    }
}

/// Signer-facing capability used to seal shaped requests.
#[derive(Debug)]
pub struct SigningCapability {
    _seal: crate::private::Seal,
}

/// Invokes a signer with a temporary signing capability.
pub async fn sign_request(
    signer: &dyn Signer,
    shaped: ShapedRequest,
) -> Result<SignedRequest, SignerError> {
    let mut capability = SigningCapability {
        _seal: crate::private::Seal,
    };
    signer.sign(shaped, &mut capability).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shaped_request_accessors_and_mutators_preserve_parts() {
        // Given
        let mut builder = ShapedRequestBuilder {
            _seal: crate::private::Seal,
        };
        let mut headers = HeaderMap::new();
        headers.insert("x-test", "one".parse().expect("header"));
        let mut shaped = builder.shaped_request(
            "https://example.test/v1/messages".parse().expect("url"),
            Method::POST,
            headers,
            Bytes::from_static(b"first"),
        );

        // When
        shaped.set_url("https://example.test/v1/complete".parse().expect("url"));
        shaped.set_method(Method::PUT);
        shaped
            .headers_mut()
            .insert("x-test", "two".parse().expect("header"));
        shaped.set_body(Bytes::from_static(b"second"));

        // Then
        assert_eq!(shaped.url().path(), "/v1/complete");
        assert_eq!(shaped.method(), Method::PUT);
        assert_eq!(shaped.headers()["x-test"], "two");
        assert_eq!(shaped.body(), &Bytes::from_static(b"second"));
    }

    #[test]
    fn signed_request_exposes_and_consumes_signed_parts() {
        // Given
        let mut builder = ShapedRequestBuilder {
            _seal: crate::private::Seal,
        };
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().expect("header"));
        let shaped = builder.shaped_request(
            "https://api.example.test/v1/messages".parse().expect("url"),
            Method::POST,
            headers,
            Bytes::from_static(b"{}"),
        );
        let mut capability = SigningCapability {
            _seal: crate::private::Seal,
        };

        // When
        let signed = SignedRequest::from_shaped(shaped, &mut capability);
        let (url, method, headers, body) = signed.into_parts();

        // Then
        assert_eq!(url.as_str(), "https://api.example.test/v1/messages");
        assert_eq!(method, Method::POST);
        assert_eq!(headers["authorization"], "Bearer token");
        assert_eq!(body, Bytes::from_static(b"{}"));
    }
}
