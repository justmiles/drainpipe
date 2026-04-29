package exporter

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	goplugin "github.com/hashicorp/go-plugin"
	"github.com/rs/zerolog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/v5/grpc/shared"
)

// Exporter wraps a Steampipe plugin for table export, communicating via gRPC
// to an out-of-process plugin binary.
type Exporter struct {
	pluginClient   *grpc.PluginClient
	goPluginClient *goplugin.Client
	pluginAlias    string
	pluginName     string
	binaryPath     string
	connectionName string
	configHCL      string
	logger         zerolog.Logger
	mu             sync.Mutex

	schemaOnce  sync.Once
	schemaCache map[string]*proto.TableSchema
	schemaErr   error
}

// New launches a Steampipe plugin binary as a child process and connects
// via gRPC using the hashicorp/go-plugin protocol.
//   - pluginAlias: short name (e.g., "aws", "azure", "cloudflare")
//   - pluginName: go-plugin Dispense name (e.g., "steampipe-plugin-aws")
//   - binaryPath: filesystem path to the plugin binary
func New(pluginAlias, pluginName, binaryPath string, logger zerolog.Logger) (*Exporter, error) {
	pluginMap := map[string]goplugin.Plugin{
		pluginName: &pluginshared.WrapperPlugin{},
	}

	goPluginClient := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig:  pluginshared.Handshake,
		Plugins:          pluginMap,
		Cmd:              exec.Command(binaryPath),
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
		Logger:           hclog.NewNullLogger(),
	})

	pluginClient, err := grpc.NewPluginClient(goPluginClient, pluginName)
	if err != nil {
		goPluginClient.Kill()
		return nil, fmt.Errorf("connecting to plugin %s: %w", pluginName, err)
	}

	return &Exporter{
		pluginClient:   pluginClient,
		goPluginClient: goPluginClient,
		pluginAlias:    pluginAlias,
		pluginName:     pluginName,
		binaryPath:     binaryPath,
		connectionName: pluginAlias,
		logger:         logger,
	}, nil
}

// Close terminates the plugin child process.
func (e *Exporter) Close() {
	if e.goPluginClient != nil {
		e.goPluginClient.Kill()
	}
}

// SetConnectionConfig configures the plugin with provider-specific credentials.
// configHCL is the HCL connection config body (can be empty for default creds).
func (e *Exporter) SetConnectionConfig(configHCL string) error {
	e.mu.Lock()
	e.configHCL = configHCL
	e.mu.Unlock()

	connectionConfig := &proto.ConnectionConfig{
		Connection:      e.connectionName,
		Plugin:          e.pluginAlias,
		PluginShortName: e.pluginAlias,
		Config:          configHCL,
		PluginInstance:  e.pluginAlias,
	}

	req := &proto.SetAllConnectionConfigsRequest{
		Configs:        []*proto.ConnectionConfig{connectionConfig},
		MaxCacheSizeMb: 50,
	}

	_, err := e.pluginClient.SetAllConnectionConfigs(req)
	if err != nil {
		return fmt.Errorf("setting connection config: %w", err)
	}

	_, err = e.pluginClient.SetCacheOptions(&proto.SetCacheOptionsRequest{Enabled: false})
	if err != nil {
		return fmt.Errorf("disabling cache: %w", err)
	}

	e.logger.Info().Str("provider", e.pluginAlias).Msg("plugin connection configured")
	return nil
}

// SetRateLimiters sends rate limiter definitions to the plugin via gRPC.
// Call after SetConnectionConfig. Definitions override any plugin-compiled
// limiters with the same name.
func (e *Exporter) SetRateLimiters(defs []*proto.RateLimiterDefinition) error {
	if len(defs) == 0 {
		return nil
	}

	req := &proto.SetRateLimitersRequest{
		Definitions: defs,
	}
	_, err := e.pluginClient.SetRateLimiters(req)
	if err != nil {
		return fmt.Errorf("setting rate limiters: %w", err)
	}

	e.logger.Info().Int("limiters", len(defs)).Msg("rate limiters configured")
	return nil
}

// fetchSchemaOnce fetches the full plugin schema exactly once and caches it.
func (e *Exporter) fetchSchemaOnce() (map[string]*proto.TableSchema, error) {
	e.schemaOnce.Do(func() {
		schema, err := e.pluginClient.GetSchema(e.connectionName)
		if err != nil {
			e.schemaErr = fmt.Errorf("getting schema: %w", err)
			return
		}
		e.schemaCache = schema.Schema
	})
	return e.schemaCache, e.schemaErr
}

// resetSchemaCache clears the cached schema so the next call re-fetches it.
// Called after Reconnect to pick up the new plugin process.
func (e *Exporter) resetSchemaCache() {
	e.schemaOnce = sync.Once{}
	e.schemaCache = nil
	e.schemaErr = nil
}

// GetSchema returns the schema for the given table.
func (e *Exporter) GetSchema(tableName string) (*proto.TableSchema, error) {
	schemas, err := e.fetchSchemaOnce()
	if err != nil {
		return nil, err
	}

	tableSchema, ok := schemas[tableName]
	if !ok {
		return nil, fmt.Errorf("table %q not found in plugin schema", tableName)
	}
	return tableSchema, nil
}

// ListTables returns the names of all tables available in the plugin.
func (e *Exporter) ListTables() ([]string, error) {
	schemas, err := e.fetchSchemaOnce()
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	return names, nil
}

// GetAllSchemas returns the full schema map for all tables in a single call.
func (e *Exporter) GetAllSchemas() (map[string]*proto.TableSchema, error) {
	return e.fetchSchemaOnce()
}

// QueryOneRow exports a table and returns just the first row.
// Useful for identity/metadata tables like aws_sts_caller_identity.
func (e *Exporter) QueryOneRow(ctx context.Context, tableName string) (Row, error) {
	tableSchema, err := e.GetSchema(tableName)
	if err != nil {
		return nil, fmt.Errorf("getting schema for %s: %w", tableName, err)
	}
	rowCh, errCh := e.Export(ctx, tableName, tableSchema.GetColumnNames(), nil)

	row, ok := <-rowCh
	if !ok {
		select {
		case err := <-errCh:
			if err != nil {
				return nil, err
			}
		default:
		}
		return nil, nil
	}

	go func() {
		for range rowCh {
		}
	}()

	return row, nil
}

// Row represents a single exported resource record as a map of column name → value.
type Row map[string]interface{}

// Export executes a table export with optional server-side filtering.
// columns is the list of column names to export (from the table schema).
// The where map specifies key column → value filters (e.g. {"status": "ACTIVE"}).
// Pass nil for no filtering. The channel is closed when the export completes.
func (e *Exporter) Export(ctx context.Context, tableName string, columns []string, where map[string]string) (<-chan Row, <-chan error) {
	rowCh := make(chan Row, 256)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		quals := buildQuals(where)
		queryContext := proto.NewQueryContext(columns, quals, -1, nil)
		req := &proto.ExecuteRequest{
			Table:        tableName,
			QueryContext: queryContext,
			CallId:       grpc.BuildCallId(),
			Connection:   e.connectionName,
			ExecuteConnectionData: map[string]*proto.ExecuteConnectionData{
				e.connectionName: {
					Limit:        queryContext.Limit,
					CacheEnabled: false,
				},
			},
		}

		stream, _, cancel, err := e.pluginClient.Execute(req)
		if err != nil {
			errCh <- fmt.Errorf("starting execute: %w", err)
			return
		}

		// Cancel the gRPC stream when the caller's context is done
		go func() {
			<-ctx.Done()
			cancel()
		}()

		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				if ctx.Err() != nil {
					errCh <- ctx.Err()
					return
				}
				errCh <- fmt.Errorf("receiving row: %w", err)
				return
			}

			row := convertRow(response.Row)
			select {
			case rowCh <- row:
			case <-ctx.Done():
				errCh <- ctx.Err()
				cancel()
				return
			}
		}
	}()

	return rowCh, errCh
}

// Exited returns whether the plugin process has terminated.
func (e *Exporter) Exited() bool {
	return e.goPluginClient.Exited()
}

// Reconnect kills the old plugin process and launches a fresh one with
// the same configuration. Safe to call from multiple goroutines; only
// one reconnect runs at a time.
func (e *Exporter) Reconnect() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.goPluginClient != nil && !e.goPluginClient.Exited() {
		e.goPluginClient.Kill()
	}

	pluginMap := map[string]goplugin.Plugin{
		e.pluginName: &pluginshared.WrapperPlugin{},
	}

	e.goPluginClient = goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig:  pluginshared.Handshake,
		Plugins:          pluginMap,
		Cmd:              exec.Command(e.binaryPath),
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
		Logger:           hclog.NewNullLogger(),
	})

	pluginClient, err := grpc.NewPluginClient(e.goPluginClient, e.pluginName)
	if err != nil {
		e.goPluginClient.Kill()
		return fmt.Errorf("reconnecting to plugin %s: %w", e.pluginName, err)
	}
	e.pluginClient = pluginClient
	e.resetSchemaCache()

	if e.configHCL != "" || e.pluginAlias != "" {
		if err := e.setConnectionConfigLocked(); err != nil {
			return fmt.Errorf("reconfiguring after reconnect: %w", err)
		}
	}

	e.logger.Info().Msg("plugin process reconnected")
	return nil
}

func (e *Exporter) setConnectionConfigLocked() error {
	connectionConfig := &proto.ConnectionConfig{
		Connection:      e.connectionName,
		Plugin:          e.pluginAlias,
		PluginShortName: e.pluginAlias,
		Config:          e.configHCL,
		PluginInstance:  e.pluginAlias,
	}

	req := &proto.SetAllConnectionConfigsRequest{
		Configs:        []*proto.ConnectionConfig{connectionConfig},
		MaxCacheSizeMb: 50,
	}

	if _, err := e.pluginClient.SetAllConnectionConfigs(req); err != nil {
		return fmt.Errorf("setting connection config: %w", err)
	}

	if _, err := e.pluginClient.SetCacheOptions(&proto.SetCacheOptionsRequest{Enabled: false}); err != nil {
		return fmt.Errorf("disabling cache: %w", err)
	}

	return nil
}

// buildQuals converts a simple map[string]string into the proto.Quals map
// needed by the Steampipe SDK. Each entry becomes an equality qual (= operator).
// Returns nil if the input map is nil or empty.
func buildQuals(where map[string]string) map[string]*proto.Quals {
	if len(where) == 0 {
		return nil
	}
	quals := make(map[string]*proto.Quals, len(where))
	for col, val := range where {
		quals[col] = &proto.Quals{
			Quals: []*proto.Qual{
				{
					FieldName: col,
					Operator:  &proto.Qual_StringValue{StringValue: "="},
					Value: &proto.QualValue{
						Value: &proto.QualValue_StringValue{StringValue: val},
					},
				},
			},
		}
	}
	return quals
}

// convertRow converts a proto.Row to a map[string]interface{}.
func convertRow(protoRow *proto.Row) Row {
	row := make(Row, len(protoRow.Columns))
	for name, col := range protoRow.Columns {
		row[name] = columnToInterface(col)
	}
	return row
}

// columnToInterface extracts the Go value from a proto.Column.
func columnToInterface(col *proto.Column) interface{} {
	switch v := col.GetValue().(type) {
	case *proto.Column_StringValue:
		return v.StringValue
	case *proto.Column_IntValue:
		return v.IntValue
	case *proto.Column_DoubleValue:
		return v.DoubleValue
	case *proto.Column_BoolValue:
		return v.BoolValue
	case *proto.Column_JsonValue:
		if len(v.JsonValue) == 0 || string(v.JsonValue) == "null" {
			return nil
		}
		return string(v.JsonValue)
	case *proto.Column_TimestampValue:
		return v.TimestampValue.AsTime()
	case *proto.Column_IpAddrValue:
		return v.IpAddrValue
	case *proto.Column_CidrRangeValue:
		return v.CidrRangeValue
	case *proto.Column_NullValue:
		return nil
	default:
		return nil
	}
}

// WaitForPlugin waits up to the given timeout for the plugin to be ready
// by polling GetSchema.
func (e *Exporter) WaitForPlugin(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := e.pluginClient.GetSchema(e.connectionName)
		if err == nil {
			return nil
		}
		if e.goPluginClient.Exited() {
			return fmt.Errorf("plugin process exited unexpectedly")
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("plugin did not become ready within %s", timeout)
}
