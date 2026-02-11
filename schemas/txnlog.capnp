@0xb81063bb62bbba0b;

enum Type {
    write @0;
    delete @1;
}

enum State {
    prepared @0;
    committed @1;
    aborted @2;
}

struct TransactionLogEntry {
    type @0 :Type;
    txid @1 :UInt64;
    key @2 :Text;
    state @3 :State;
}
