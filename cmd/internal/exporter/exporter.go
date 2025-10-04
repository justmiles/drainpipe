package exporter

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/turbot/steampipe-plugin-sdk/v5/anywhere"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// Exporter wraps a Steampipe plugin server for in-process execution.
// It is provider-agnostic — the plugin function and alias are passed in.
type Exporter struct {
	server         *grpc.PluginServer
	pluginAlias    string
	connectionName string
	logger         zerolog.Logger
}

// New creates a new Exporter for the given provider.
//   - pluginAlias: short name (e.g., "aws", "azure", "cloudflare")
//   - pluginFunc: the Steampipe plugin constructor
func New(pluginAlias string, pluginFunc plugin.PluginFunc, logger zerolog.Logger) *Exporter {
	server := plugin.Server(&plugin.ServeOpts{
		PluginFunc: pluginFunc,
	})

	return &Exporter{
		server:         server,
		pluginAlias:    pluginAlias,
		connectionName: pluginAlias, // connection name defaults to alias
		logger:         logger,
	}
}

// SetConnectionConfig configures the plugin with provider-specific credentials.
// configHCL is the HCL connection config body (can be empty for default creds).
func (e *Exporter) SetConnectionConfig(configHCL string) error {
	connectionConfig := &proto.ConnectionConfig{
		Connection:      e.connectionName,
		Plugin:          e.pluginAlias,
		PluginShortName: e.pluginAlias,
		Config:          configHCL,
		PluginInstance:  e.pluginAlias,
	}

	req := &proto.SetAllConnectionConfigsRequest{
		Configs:        []*proto.ConnectionConfig{connectionConfig},
		MaxCacheSizeMb: -1,
	}

	_, err := e.server.SetAllConnectionConfigs(req)
	if err != nil {
		return fmt.Errorf("setting connection config: %w", err)
	}

	// Disable cache — we're doing batch exports, not interactive queries
	_, err = e.server.SetCacheOptions(&proto.SetCacheOptionsRequest{Enabled: false})
	if err != nil {
		return fmt.Errorf("disabling cache: %w", err)
	}

	e.logger.Info().Str("provider", e.pluginAlias).Msg("plugin connection configured")
	return nil
}

// GetSchema returns the schema for the given table.
func (e *Exporter) GetSchema(tableName string) (*proto.TableSchema, error) {
	req := &proto.GetSchemaRequest{
		Connection: e.connectionName,
	}
	resp, err := e.server.GetSchema(req)
	if err != nil {
		return nil, fmt.Errorf("getting schema: %w", err)
	}

	tableSchema, ok := resp.Schema.Schema[tableName]
	if !ok {
		return nil, fmt.Errorf("table %q not found in plugin schema", tableName)
	}
	return tableSchema, nil
}

// ListTables returns the names of all tables available in the plugin.
func (e *Exporter) ListTables() ([]string, error) {
	req := &proto.GetSchemaRequest{
		Connection: e.connectionName,
	}
	resp, err := e.server.GetSchema(req)
	if err != nil {
		return nil, fmt.Errorf("getting schema: %w", err)
	}

	names := make([]string, 0, len(resp.Schema.Schema))
	for name := range resp.Schema.Schema {
		names = append(names, name)
	}
	return names, nil
}

// GetAllSchemas returns the full schema map for all tables in a single call.
// Use this when you need schemas for many tables to avoid repeated GetSchema calls.
func (e *Exporter) GetAllSchemas() (map[string]*proto.TableSchema, error) {
	req := &proto.GetSchemaRequest{
		Connection: e.connectionName,
	}
	resp, err := e.server.GetSchema(req)
	if err != nil {
		return nil, fmt.Errorf("getting schema: %w", err)
	}
	return resp.Schema.Schema, nil
}

// QueryOneRow exports a table and returns just the first row.
// Useful for identity/metadata tables like aws_sts_caller_identity.
func (e *Exporter) QueryOneRow(ctx context.Context, tableName string) (Row, error) {
	rowCh, errCh := e.Export(ctx, tableName, nil)

	// Take the first row
	row, ok := <-rowCh
	if !ok {
		// Channel closed with no rows — check for error
		select {
		case err := <-errCh:
			if err != nil {
				return nil, err
			}
		default:
		}
		return nil, nil
	}

	// Drain remaining rows (we only want one)
	go func() {
		for range rowCh {
		}
	}()

	return row, nil
}

// Row represents a single exported resource record as a map of column name → value.
type Row map[string]interface{}

// Export executes a table export with optional server-side filtering.
// The where map specifies key column → value filters (e.g. {"status": "ACTIVE"}).
// Pass nil for no filtering. The channel is closed when the export completes.
func (e *Exporter) Export(ctx context.Context, tableName string, where map[string]string) (<-chan Row, <-chan error) {
	rowCh := make(chan Row, 256)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		// Get schema to know all columns
		schema, err := e.GetSchema(tableName)
		if err != nil {
			errCh <- err
			return
		}

		columns := schema.GetColumnNames()

		// Build quals from the where map
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

		stream := anywhere.NewLocalPluginStream(ctx)
		e.server.CallExecuteAsync(req, stream)

		// Wait for stream to be ready
		select {
		case <-stream.Ready():
		case <-ctx.Done():
			errCh <- ctx.Err()
			return
		}

		for {
			response, err := stream.Recv()
			if err != nil {
				errCh <- fmt.Errorf("receiving row: %w", err)
				return
			}
			if response == nil {
				return // stream complete
			}

			row := convertRow(response.Row)
			select {
			case rowCh <- row:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return rowCh, errCh
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
