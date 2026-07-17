use std::process::ExitCode;

fn main() -> ExitCode {
    let mut arguments = std::env::args();
    let _program = arguments.next();
    match (arguments.next().as_deref(), arguments.next()) {
        (Some("ok"), None) => ExitCode::SUCCESS,
        (Some("panic"), None) => {
            eprintln!("thread 'main' panicked");
            ExitCode::from(101)
        }
        _ => {
            eprintln!("usage: stress_dummy <ok|panic>");
            ExitCode::from(2)
        }
    }
}
