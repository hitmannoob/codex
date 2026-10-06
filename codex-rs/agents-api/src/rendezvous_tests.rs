use super::*;
use pretty_assertions::assert_eq;

#[test]
fn frames_route_by_stream_and_resets_are_recognized() {
    // RelayMessageFrame{version: 1, stream_id: "s-1", resume: {next_seq: 0}}.
    let resume = [0x08, 0x01, 0x12, 0x03, b's', b'-', b'1', 0x3a, 0x00];
    assert_eq!(stream_of(&resume), Some(("s-1".to_string(), false)));
    assert_eq!(
        stream_of(&reset("s-1", "harness_disconnected")),
        Some(("s-1".to_string(), true))
    );
    // A frame without a stream, and a truncated frame, are not routed.
    assert_eq!(stream_of(&[0x08, 0x01]), None);
    assert_eq!(stream_of(&[0x08, 0x01, 0x12, 0x09, b's']), None);
}
