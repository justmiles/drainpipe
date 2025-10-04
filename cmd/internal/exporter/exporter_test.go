package exporter

import (
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestColumnToInterface_String(t *testing.T) {
	col := &proto.Column{Value: &proto.Column_StringValue{StringValue: "hello"}}
	if got := columnToInterface(col); got != "hello" {
		t.Errorf("got %v, want %q", got, "hello")
	}
}

func TestColumnToInterface_Int(t *testing.T) {
	col := &proto.Column{Value: &proto.Column_IntValue{IntValue: 42}}
	if got := columnToInterface(col); got != int64(42) {
		t.Errorf("got %v, want 42", got)
	}
}

func TestColumnToInterface_Double(t *testing.T) {
	col := &proto.Column{Value: &proto.Column_DoubleValue{DoubleValue: 3.14}}
	if got := columnToInterface(col); got != 3.14 {
		t.Errorf("got %v, want 3.14", got)
	}
}

func TestColumnToInterface_Bool(t *testing.T) {
	col := &proto.Column{Value: &proto.Column_BoolValue{BoolValue: true}}
	if got := columnToInterface(col); got != true {
		t.Errorf("got %v, want true", got)
	}
}

func TestColumnToInterface_JSON(t *testing.T) {
	col := &proto.Column{Value: &proto.Column_JsonValue{JsonValue: []byte(`{"k":"v"}`)}}
	if got := columnToInterface(col); got != `{"k":"v"}` {
		t.Errorf("got %v, want JSON string", got)
	}
}

func TestColumnToInterface_Timestamp(t *testing.T) {
	ts := timestamppb.New(time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC))
	col := &proto.Column{Value: &proto.Column_TimestampValue{TimestampValue: ts}}
	got, ok := columnToInterface(col).(time.Time)
	if !ok {
		t.Fatalf("type = %T, want time.Time", columnToInterface(col))
	}
	if !got.Equal(time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)) {
		t.Errorf("got %v", got)
	}
}

func TestColumnToInterface_IpAddr(t *testing.T) {
	col := &proto.Column{Value: &proto.Column_IpAddrValue{IpAddrValue: "10.0.0.1"}}
	if got := columnToInterface(col); got != "10.0.0.1" {
		t.Errorf("got %v", got)
	}
}

func TestColumnToInterface_CidrRange(t *testing.T) {
	col := &proto.Column{Value: &proto.Column_CidrRangeValue{CidrRangeValue: "10.0.0.0/24"}}
	if got := columnToInterface(col); got != "10.0.0.0/24" {
		t.Errorf("got %v", got)
	}
}

func TestColumnToInterface_Null(t *testing.T) {
	col := &proto.Column{Value: &proto.Column_NullValue{}}
	if got := columnToInterface(col); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestColumnToInterface_NilValue(t *testing.T) {
	col := &proto.Column{}
	if got := columnToInterface(col); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestConvertRow(t *testing.T) {
	row := convertRow(&proto.Row{
		Columns: map[string]*proto.Column{
			"name":   {Value: &proto.Column_StringValue{StringValue: "bucket"}},
			"public": {Value: &proto.Column_BoolValue{BoolValue: false}},
			"count":  {Value: &proto.Column_IntValue{IntValue: 100}},
		},
	})
	if row["name"] != "bucket" {
		t.Errorf("row[name] = %v", row["name"])
	}
	if row["public"] != false {
		t.Errorf("row[public] = %v", row["public"])
	}
	if row["count"] != int64(100) {
		t.Errorf("row[count] = %v", row["count"])
	}
}

func TestConvertRow_Empty(t *testing.T) {
	row := convertRow(&proto.Row{Columns: map[string]*proto.Column{}})
	if len(row) != 0 {
		t.Errorf("len = %d, want 0", len(row))
	}
}

func TestBuildQuals_Nil(t *testing.T) {
	got := buildQuals(nil)
	if got != nil {
		t.Errorf("buildQuals(nil) = %v, want nil", got)
	}
}

func TestBuildQuals_Empty(t *testing.T) {
	got := buildQuals(map[string]string{})
	if got != nil {
		t.Errorf("buildQuals(empty) = %v, want nil", got)
	}
}

func TestBuildQuals_SingleEntry(t *testing.T) {
	got := buildQuals(map[string]string{"status": "ACTIVE"})
	if got == nil {
		t.Fatal("buildQuals returned nil")
	}
	quals, ok := got["status"]
	if !ok {
		t.Fatal("missing 'status' key in quals map")
	}
	if len(quals.Quals) != 1 {
		t.Fatalf("len(quals.Quals) = %d, want 1", len(quals.Quals))
	}
	q := quals.Quals[0]
	if q.FieldName != "status" {
		t.Errorf("FieldName = %q, want %q", q.FieldName, "status")
	}
	if q.GetStringValue() != "=" {
		t.Errorf("Operator = %q, want %q", q.GetStringValue(), "=")
	}
	if q.Value.GetStringValue() != "ACTIVE" {
		t.Errorf("Value = %q, want %q", q.Value.GetStringValue(), "ACTIVE")
	}
}

func TestBuildQuals_MultipleEntries(t *testing.T) {
	got := buildQuals(map[string]string{"status": "ACTIVE", "region": "us-east-1"})
	if got == nil {
		t.Fatal("buildQuals returned nil")
	}
	if len(got) != 2 {
		t.Fatalf("len(quals) = %d, want 2", len(got))
	}
	if _, ok := got["status"]; !ok {
		t.Error("missing 'status' key")
	}
	if _, ok := got["region"]; !ok {
		t.Error("missing 'region' key")
	}
}
