use axum::body::Body;
use axum::http::{Request, Response, StatusCode, header};
use tower_http::auth::AsyncAuthorizeRequest;
use std::future::Future;
use std::pin::Pin;
use base64::{engine::general_purpose, Engine};

#[derive(Clone)]
pub struct BasicAuth {
    expected: String,
}

impl BasicAuth {
    /// 创建 Basic 认证器
    ///
    /// 注意：
    /// - username / password 允许为空
    /// - 是否启用认证应在 Router 层判断
    pub fn new(username: Option<String>, password: Option<String>) -> Self {
        let user = username.unwrap_or_default();
        let pass = password.unwrap_or_default();

        let raw = format!("{user}:{pass}");
        let encoded = general_purpose::STANDARD.encode(raw);

        Self {
            expected: format!("Basic {encoded}"),
        }
    }
}

impl<B> AsyncAuthorizeRequest<B> for BasicAuth
where
    B: Send + 'static,
{
    type RequestBody = B;
    type ResponseBody = Body;

    type Future =
        Pin<Box<dyn Future<Output = Result<Request<B>, Response<Self::ResponseBody>>> + Send>>;

    fn authorize(&mut self, request: Request<B>) -> Self::Future {
        let expected = self.expected.clone();

        Box::pin(async move {
            match request.headers().get(header::AUTHORIZATION) {
                Some(value) if value.as_bytes() == expected.as_bytes() => Ok(request),
                _ => Err(unauthorized()),
            }
        })
    }
}

fn unauthorized() -> Response<Body> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            header::WWW_AUTHENTICATE,
            r#"Basic realm="rtmp-forwarder""#,
        )
        .body(Body::empty())
        .unwrap()
}
