unit Aurelius.Drivers.ZeosDbc;

{$I Aurelius.inc}

interface

uses
  Classes,
  Generics.Collections,
  Db,
  ZConnection,
  ZDbcIntfs,
  Aurelius.Drivers.Interfaces,
  Aurelius.Drivers.Base;

type
  TZeosDbcConnectionAdapter = class(
    TDriverConnectionAdapter<TZConnection>, IDBConnection)
  public
    procedure Connect;
    procedure Disconnect;
    function IsConnected: Boolean;
    function BeginTransaction: IDBTransaction;
    function CreateStatement: IDBStatement;
    function RetrieveSqlDialect: string; override;
  end;

  TZeosDbcTransactionAdapter = class(TInterfacedObject, IDBTransaction)
  private
    FConnection: IZConnection;
  public
    constructor Create(const AConnection: IZConnection);
    function InTransaction: Boolean;
    procedure Commit;
    procedure Rollback;
  end;

  TZeosDbcStatementAdapter = class(
    TInterfacedObject, IDBStatement, IDBDatasetStatement)
  private
    FConnection: IZConnection;
    FStatement: IZPreparedStatement;
    FParsedSQL: string;
    FParams: TParams;
    procedure PrepareStatement;
    function IsPrepared: Boolean;
  protected
    function GetDataset: TDataSet; virtual;
  public
    constructor Create(const AConnection: IZConnection);
    destructor Destroy; override;
    procedure SetSQLCommand({const} ASQLCommand: string);
    procedure SetParams(ADBParams: TEnumerable<TDBParam>);
    function Execute: Integer;
    function ExecuteQuery: IDBResultSet;
  end;

  TZeosDbcResultSetAdapter = class(
    TInterfacedObject, IDBResultSet, IDBResultSet2)
  private
    FFetching: Boolean;
  protected
    FResultSet: IZResultSet;
  public
    constructor Create(const AResultSet: IZResultSet);
    destructor Destroy; override;
    function Next: Boolean;
    function GetFieldValue({const} AFieldName: string): Variant; overload;
    function GetFieldValue({const} AFieldIndex: Integer): Variant; overload;
    function GetFieldIndex(const AFieldName: string): Integer;
  end;

implementation

uses
  SysUtils,
  StrUtils,
  Variants,
  ZVariant,
  ZDatasetUtils,
  Aurelius.Global.Utils,
  Aurelius.Global.Exceptions,
  Aurelius.Drivers.Exceptions,
  Aurelius.Comp.Adapters;

type
  EStatementNotPrepared = class(EOPFBaseException)
  public
    constructor Create;
  end;

  EUnexpectedParam = class(EOPFBaseException)
  public
    constructor Create(AParamIndex: Integer; const AParamName: string='');
  end;

  TParamHelper = class helper for TParam
    procedure AssignDBParam(ADBParam: TDBParam);
  end;

{ EStatementNotPrepared }

constructor EStatementNotPrepared.Create;
begin
  inherited Create('SQL command not defined or statement not prepared');
end;

{ EUnexpectedParam }

constructor EUnexpectedParam.Create(
  AParamIndex: Integer; const AParamName: string);
const
  S_UnexpectedParamName = 'Unexpected param name "%s" at index %d';
  S_UnexpectedParamIndex = 'Unexpected param at index %d';
var
  LMessage: string;
begin
  if AParamName<>'' then
    LMessage := Format(S_UnexpectedParamName, [AParamName, AParamIndex])
  else
    LMessage := Format(S_UnexpectedParamIndex, [AParamIndex]);
  inherited Create(LMessage);
end;

{ TParamHelper }

procedure TParamHelper.AssignDBParam(ADBParam: TDBParam);
var
  Bytes: TBytes;
  Guid: TGuid;
begin
  if ADBParam.ParamType in [ftBlob, ftOraBlob, ftOraClob] then
  begin
    Self.DataType := ADBParam.ParamType;
    Bytes := TUtils.VariantToBytes(ADBParam.ParamValue);
    if VarIsNull(ADBParam.ParamValue) or (Length(Bytes)=0) then
      Self.Clear
    else
      Self.AsBlob := Bytes;
  end else
  if (ADBParam.ParamType in [ftGuid]) then
  begin
    Guid := TUtils.VariantToGuid(ADBParam.ParamValue);
    if TUtils.IsNullGuid(Guid) then
      Self.Clear
    else
      Self.AsString := GuidToString(Guid);
  end else
  begin
    Self.DataType := ADBParam.ParamType;
    Self.Value := ADBParam.ParamValue;
  end;
end;

{ TZeosDbcConnectionAdapter }

procedure TZeosDbcConnectionAdapter.Connect;
begin
  if (Connection<>nil) and (not Connection.Connected) then
  begin
    {
    }
    Connection.Connected := True;
  end;
end;

procedure TZeosDbcConnectionAdapter.Disconnect;
begin
  if Connection<>nil then
    Connection.Connected := False;
end;

function TZeosDbcConnectionAdapter.IsConnected: Boolean;
begin
  if Connection<>nil then
    Result := Connection.Connected
  else
    Result := False;
end;

function TZeosDbcConnectionAdapter.BeginTransaction: IDBTransaction;
begin
  if Connection<>nil then
  begin
    Connect;
    if not Connection.InTransaction then
      Connection.StartTransaction;
    Result := TZeosDbcTransactionAdapter.Create(Connection.DbcConnection);
  end;
  {
  or, should be:

  if Connection<>nil then
  begin
    Connect;
    if not Connection.InTransaction then
    begin
      Connection.StartTransaction;
      Result := TZeosDbcTransactionAdapter.Create(Connection.DbcConnection);
    end else
      Result := TZeosDbcTransactionAdapter.Create(nil); // ?
  end;
  }
end;

function TZeosDbcConnectionAdapter.CreateStatement: IDBStatement;
begin
  if Connection<>nil then
  begin
    Connect;
    {
    if not Connection.InTransaction then
      BeginTransaction;
    }
    Result := TZeosDbcStatementAdapter.Create(Connection.DbcConnection);
  end;
end;

function TZeosDbcConnectionAdapter.RetrieveSqlDialect: string;
var
  LProtocol: string;
begin
  if Connection<>nil then
  begin
    LProtocol := LowerCase(Connection.Protocol);
    if ContainsStr(LProtocol, 'firebird3') then
      Result := 'Firebird3'
    else
    if ContainsStr(LProtocol, 'firebird') then
      Result := 'Firebird'
    else
    if ContainsStr(LProtocol, 'interbase') then
      Result := 'Interbase'
    else
    if ContainsStr(LProtocol, 'mysql') then
      Result := 'MySQL'
    else
    if ContainsStr(LProtocol, 'oracle') then
      Result := 'Oracle'
    else
    if ContainsStr(LProtocol, 'postgre') then
      Result := 'PostgreSQL'
    else
    if ContainsStr(LProtocol, 'mssql')
      or ContainsStr(LProtocol, 'oledb')
      or ContainsStr(LProtocol, 'ado') then
    begin
      Result := 'MSSQL';
    end;
  end;
end;

{ TZeosDbcTransactionAdapter }

constructor TZeosDbcTransactionAdapter.Create(const AConnection: IZConnection);
begin
  inherited Create;
  FConnection := AConnection;
  {
  if not InTransaction then
    FConnection.StartTransaction;
  }
end;

function TZeosDbcTransactionAdapter.InTransaction: Boolean;
begin
  if Assigned(FConnection) then
    Result := (FConnection.GetConnectionTransaction<>nil) {and (FConnection.GetAutoCommit=False)}
  else
    Result := False;
end;

procedure TZeosDbcTransactionAdapter.Commit;
begin
  if InTransaction then // while do
    FConnection.Commit;
end;

procedure TZeosDbcTransactionAdapter.Rollback;
begin
  if InTransaction then // while do
    FConnection.Rollback;
end;

{ TZeosDbcStatementAdapter }

constructor TZeosDbcStatementAdapter.Create(const AConnection: IZConnection);
begin
  inherited Create;
  FParams := TParams.Create(nil);
  FConnection := AConnection;
  FParsedSQL := '';
end;

destructor TZeosDbcStatementAdapter.Destroy;
begin
  FStatement := nil;
  FParams.Free;
  inherited;
end;

procedure TZeosDbcStatementAdapter.PrepareStatement;
begin
  if FStatement=nil then
    FStatement := FConnection.PrepareStatement(FParsedSQL);
end;

function TZeosDbcStatementAdapter.IsPrepared: Boolean;
begin
  {
  PrepareStatement;
  }
  Result := Assigned(FStatement);
end;

procedure TZeosDbcStatementAdapter.SetSQLCommand({const} ASQLCommand: string);
begin
  FStatement := nil;
  FParams.Clear;
  FParsedSQL := FParams.ParseSQL(Trim(ASQLCommand), True);
  PrepareStatement;
end;

procedure TZeosDbcStatementAdapter.SetParams(ADBParams: TEnumerable<TDBParam>);
var
  I: Integer;
  P: TDBParam;
  LParam: TParam;
  LIndex: Integer;
begin
  I := 0;
  for P in ADBParams do
  begin
    if P.ParamName='' then
    begin
      if I<FParams.Count then
        LParam := FParams[I]
      else
        LParam := nil;
    end else
      LParam := FParams.FindParam(P.ParamName);
    if Assigned(LParam) then
      LParam.AssignDBParam(P)
    else
      raise EUnexpectedParam.Create(I, P.ParamName);
    Inc(I);
  end;
  if IsPrepared then
  begin
    for I := 0 to FParams.Count-1 do
    begin
      LIndex := I{$IFNDEF GENERIC_INDEX}+1{$ENDIF};
      LParam := FParams[I];
      SetStatementParam(LIndex, FStatement, LParam);
    end;
  end else
    raise EStatementNotPrepared.Create;
end;

function TZeosDbcStatementAdapter.Execute: Integer;
begin
  if IsPrepared then
  begin
    Result := FStatement.ExecuteUpdatePrepared;
  end else
    raise EStatementNotPrepared.Create;
end;

function TZeosDbcStatementAdapter.ExecuteQuery: IDBResultSet;
var
  LResultSet: IZResultSet;
begin
  if IsPrepared then
  begin
    LResultSet := FStatement.ExecuteQueryPrepared;
    Result := TZeosDbcResultSetAdapter.Create(LResultSet);
  end else
    raise EStatementNotPrepared.Create;
end;

function TZeosDbcStatementAdapter.GetDataset: TDataSet;
begin
  raise EFeatureNotSupported.Create('GetDataSet');
end;

{ TZeosDbcResultSetAdapter }

constructor TZeosDbcResultSetAdapter.Create(const AResultSet: IZResultSet);
begin
  inherited Create;
  FResultSet := AResultSet;
  FFetching := False;
end;

destructor TZeosDbcResultSetAdapter.Destroy;
begin
  FResultSet := nil;
  inherited;
end;

function TZeosDbcResultSetAdapter.Next: Boolean;
begin
  if not FFetching then
    FFetching := True;
  Result := FResultSet.Next;
end;

function TZeosDbcResultSetAdapter.GetFieldIndex(const AFieldName: string): Integer;
begin
  Result := FResultSet.FindColumn(AFieldName){$IFNDEF GENERIC_INDEX}-1{$ENDIF};
end;

function TZeosDbcResultSetAdapter.GetFieldValue({const} AFieldName: string): Variant;
begin
  Result := EncodeVariant(FResultSet.GetValueByName(AFieldName));
end;

function TZeosDbcResultSetAdapter.GetFieldValue(AFieldIndex: Integer): Variant;
var
  LIndex: Integer;
begin
  LIndex := AFieldIndex{$IFNDEF GENERIC_INDEX}+1{$ENDIF};
  Result := EncodeVariant(FResultSet.GetValue(LIndex));
end;

{ RegisterDriverAdapter }

procedure RegisterDriverAdapter;
begin
  with TAureliusAdapterRegister.Instance do
  begin
    RegisterAdapter(IAureliusAdapter(
      TAureliusAdapter.Create(
        'ZeosDbc', 'TZConnection', 'Firebird,Firebird3,MSSQL,MySQL,Oracle,PostgreSQL'))
    );
    RegisterAdapterFactory('ZeosDbc',
      function(Args: TAdapterFactoryArgs): IDBConnection
      begin
        Result := TZeosDbcConnectionAdapter.Create(
          Args.AdaptedConnection as TZConnection, Args.SqlDialect, Args.Owner);
      end
    );
  end;
end;

initialization
  RegisterDriverAdapter;

end.
