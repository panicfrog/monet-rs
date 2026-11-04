#pragma once

// The Rust battery driver (crates/monet-demo, the apple module): runs the
// shared monet-conformance battery over the native transport and returns
// `name: OK/FAILED` lines plus a summary. Free with monet_demo_free_string.
char *monet_demo_run_battery(void);
void monet_demo_free_string(char *text);
