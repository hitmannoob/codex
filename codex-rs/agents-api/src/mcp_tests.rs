use super::*;
use pretty_assertions::assert_eq;

#[test]
fn only_public_addresses_are_reachable_without_approval() {
    let reachable = [
        "93.184.216.34",
        "8.8.8.8",
        "2606:4700:4700::1111",
        "::ffff:8.8.8.8",
    ];
    let blocked = [
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "100.64.0.1",
        "0.0.0.0",
        "255.255.255.255",
        "::1",
        "fd00::1",
        "fe80::1",
        "::ffff:127.0.0.1",
        "::ffff:169.254.169.254",
    ];
    let classify = |addresses: &[&str]| {
        addresses
            .iter()
            .map(|address| public(&address.parse().expect("address")))
            .collect::<Vec<_>>()
    };
    assert_eq!(classify(&reachable), vec![true; reachable.len()]);
    assert_eq!(classify(&blocked), vec![false; blocked.len()]);
}
