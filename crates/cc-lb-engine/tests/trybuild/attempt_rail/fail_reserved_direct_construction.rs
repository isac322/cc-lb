use cc_lb_engine::attempt_rail::AttemptIntent;

fn invalid_flow() {
    let _intent = AttemptIntent::from_reservation(None);
}

fn main() {}
