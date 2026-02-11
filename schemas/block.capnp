@0xdddd5102e9e4d318;

enum Compression {
  none @0;
  zstd @1;
}

enum Hash {
  xxh3 @0;
}

struct FileHeader {
  magic @0 :UInt32 = 0x594F5445;
  version @1 :UInt8 = 0;
  blockSize @2 :UInt32;
  nBlocks @3 :UInt32;
  hashType @4 :Hash;
  compressionType @5 :Compression;
}

struct Block {
  checksum @0 :UInt64;
  data @1 :Data;
}
