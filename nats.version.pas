unit nats.version;

{$IFDEF FPC}
  {$mode delphi}
{$ENDIF}

interface

const
  NATS_VERSION_MAJOR  = 3;
  NATS_VERSION_MINOR  = 3;
  NATS_VERSION_PATCH  = 0;

  NATS_VERSION_STRING = '3.3.0';

  NATS_VERSION_NUMBER = NATS_VERSION_MAJOR shl 16 +
                        NATS_VERSION_MINOR shl  8 +
                        NATS_VERSION_PATCH;

  NATS_VERSION_REQUIRED_NUMBER = $030300;

implementation

end.

