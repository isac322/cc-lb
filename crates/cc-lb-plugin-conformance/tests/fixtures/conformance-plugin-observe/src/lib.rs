#[cc_lb_pdk::plugin(
    name = "conformance-observe",
    version = "0.0.0-test",
    requires = ["observability:emit"]
)]
mod plugin {
    use cc_lb_plugin_wire::v2::observe::{ObserveRequest, ObserveResponse};
    use std::convert::Infallible;

    #[cc_lb_pdk::handler(name = "observe", versions = [1])]
    pub(super) fn observe_handler(_request: ObserveRequest) -> Result<ObserveResponse, Infallible> {
        Ok(ObserveResponse {})
    }
}
