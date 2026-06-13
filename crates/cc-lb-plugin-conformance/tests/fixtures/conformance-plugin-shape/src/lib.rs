use cc_lb_plugin_wire::v1::shape::{ShapeRequest, ShapeResponse};
use std::convert::Infallible;

#[cc_lb_pdk::plugin(name = "conformance-shape", version = "0.0.0-test")]
mod plugin {
    use super::{Infallible, ShapeRequest, ShapeResponse};

    #[cc_lb_pdk::handler(name = "shape", versions = [1])]
    pub(super) fn shape_handler(_request: ShapeRequest) -> Result<ShapeResponse, Infallible> {
        Ok(ShapeResponse {
            url: "https://api.anthropic.com/v1/messages".to_string(),
            method: "POST".to_string(),
            headers: Vec::new(),
            body_base64: String::new(),
        })
    }
}
