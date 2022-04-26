program publisher;
// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

uses
  nats.core, nats.status;

type
  TStatsEnum = (
    STATS_IN,
    STATS_OUT,
    STATS_COUNT
  );

  TStatsModes = set of TStatsEnum;

const
  servers: array[0..0] of string = ('127.0.0.1');
  subject = 'test-pas';
  payload = 'hello';
  name = 'worker';

var
  async: Boolean = True;
  total: Int64   = 1000000;

  count: Int64   = 0;
  dropped: Int64 = 0;
  start: Int64   = 0;
  elapsed: Int64 = 0;
  print: Boolean = False;
  timeout: Int64 = 10000; // 10 seconds.

  opts: PnatsOptions;

  certFile: AnsiString;
  keyFile: AnsiString;

  cluster: AnsiString   = 'test-cluster';
  clientID: AnsiString  = 'client';
  qgroup: AnsiString;
  durable: AnsiString;
  deliverAll: Boolean   = False;
  deliverLast: Boolean  = True;
  deliverSeq: UInt64    = 0;
  unsubscribe: Boolean  = False;

  stream: AnsiString;
  pull: Boolean         = False;
  flowctrl: Boolean     = False;

function printStats(mode: TStatsModes; conn: PnatsConnection; sub: PnatsSubscription; stats: PnatsStatistics): natsStatus;
var
  inMsgs: UInt64;
  inBytes: UInt64;
  outMsgs: UInt64;
  outBytes: UInt64;
  reconnected: UInt64;
  pending: Integer;
  delivered: Int64;
  sdropped: Int64;
begin
  Result := natsConnection_GetStats(conn, stats);
  if Result = NATS_OK then
    Result := natsStatistics_GetCounts(stats, inMsgs, inBytes, outMsgs, outBytes, reconnected);
  if (Result = NATS_OK) and (sub <> nil) then begin
    Result := natsSubscription_GetStats(sub, @pending, nil, nil, nil, @delivered, @sdropped);

    // Since we use AutoUnsubscribe(), when the max has been reached,
    // the subscription is automatically closed, so this call would
    // return "Invalid Subscription". Ignore this error.
    if Result = NATS_INVALID_SUBSCRIPTION then begin
      Result := NATS_OK;
      inMsgs := 0;
      inBytes := 0;
      outMsgs := 0;
      outBytes := 0;
      reconnected := 0;
      pending := 0;
      delivered := 0;
      sdropped := 0;
    end;
  end;

  if Result = NATS_OK then begin
    if STATS_IN in mode then
      Write('InMsgs:', inMsgs:9, ' - InBytes:', inBytes:9, ' - ');
    if STATS_OUT in mode then
      Write('OutMsgs:', outMsgs:9, ' - OutBytes:', outBytes:9, ' - ');
    if STATS_COUNT in mode then
      Write('Delivered:', delivered:6, ' - Pending:', pending:6, 'Dropped:', sdropped:6, ' - ');
    Writeln('Reconnected:', reconnected:3);
  end;
end;

procedure printPerf(perfTxt: AnsiString);
begin
  if (start > 0) and (elapsed = 0) then
    elapsed := nats_Now - start;

  if elapsed <= 0 then
    Writeln(LineEnding, 'Not enough messages or too fast to report performance!')
  else
    Writeln(LineEnding, perfTxt, ' ', count, ' messages in ',
            elapsed, ' milliseconds (', (count * 1000) div elapsed, ' msgs/sec)');
end;

var
  options: PnatsOptions;
  conn: PnatsConnection;
  stats: PnatsStatistics;
  sub: PnatsSubscription;
  s: natsStatus;
  dataLen: Integer;
  i: Integer;
  ErrorStack: array[0..$7FFF] of AnsiChar;
begin
  s := natsOptions_Create(options);
  if s = NATS_OK then
    s := natsOptions_SetServers(options, @servers, 1);
  if s = NATS_OK then
    s := natsOptions_SetSecure(options, true);

  if s = NATS_OK then
    s := natsConnection_Connect(conn, options);

  if s = NATS_OK then
    s := natsStatistics_Create(stats);

  if s = NATS_OK then
    start := nats_Now;

  dataLen := Length(payload);
  for i := 1 to 1000000 do begin
    s := natsConnection_Publish(conn, PAnsiChar(subject), PAnsiChar(payload), dataLen);
    if s <> NATS_OK then
      Break;
  end;

  if s = NATS_OK then
    s := natsConnection_FlushTimeout(conn, 1000);

  if s = NATS_OK then begin
    printStats([STATS_OUT], conn, nil, stats);
    printPerf('Sent');
  end
  else begin
    Writeln('Error: ', s, ' - ', natsStatus_GetText(s));
    if nats_GetLastErrorStack(PAnsiChar(ErrorStack), SizeOf(ErrorStack)) = NATS_OK then begin
      Writeln(ErrorStack);
    end;
  end;

  // Destroy all our objects to avoid report of memory leak
  natsStatistics_Destroy(stats);
  natsConnection_Destroy(conn);
  natsOptions_Destroy(options);

  // To silence reports of memory still in used with valgrind
  nats_Close();
end.

