namespace java com.uber.tchannel.crossdock.thrift

struct Data {
  1: required bool b1,
  2: required string s2,
  3: required i32 i3
}

service SimpleService {
  Data Call(1: Data arg)
}

