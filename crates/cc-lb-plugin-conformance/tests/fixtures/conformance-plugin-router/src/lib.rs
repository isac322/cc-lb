#[cc_lb_pdk::plugin(name = "conformance-plugin-router", version = "0.1.0")]
mod plugin {
    use cc_lb_plugin_wire::v3::filter::{FilterRequest, FilterResponse, PerCandidateReasonWire};
    use std::convert::Infallible;

    #[cc_lb_pdk::handler(name = "filter", versions = [1])]
    pub(super) fn filter_handler(request: FilterRequest) -> Result<FilterResponse, Infallible> {
        Ok(FilterResponse {
            results: request
                .candidates
                .into_iter()
                .map(|candidate| PerCandidateReasonWire {
                    upstream_id: candidate.upstream_id,
                    decision: "accept".to_string(),
                    reason: "conformance-fixture".to_string(),
                })
                .collect(),
        })
    }
}
