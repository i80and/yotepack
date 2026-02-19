@0xb81063bb62bbba0b;

enum State {
    prepared @0;
    committed @1;
    aborted @2;
}

struct TransactionLogEntry {
    txid @0 :UInt64;
    key @1 :Text;
    state @2 :State;
}
