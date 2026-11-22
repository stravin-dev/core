package ddbsync

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strings"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// ConvertAttributeMap converts a DynamoDB Streams attribute map into a DynamoDB attribute map.
func ConvertAttributeMap(in map[string]streamtypes.AttributeValue) (map[string]ddbtypes.AttributeValue, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]ddbtypes.AttributeValue, len(in))
	for k, v := range in {
		converted, err := convertAttributeValue(v)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", k, err)
		}
		out[k] = converted
	}
	return out, nil
}

func convertAttributeValue(v streamtypes.AttributeValue) (ddbtypes.AttributeValue, error) {
	switch av := v.(type) {
	case *streamtypes.AttributeValueMemberS:
		return &ddbtypes.AttributeValueMemberS{Value: av.Value}, nil
	case *streamtypes.AttributeValueMemberN:
		return &ddbtypes.AttributeValueMemberN{Value: av.Value}, nil
	case *streamtypes.AttributeValueMemberB:
		cp := make([]byte, len(av.Value))
		copy(cp, av.Value)
		return &ddbtypes.AttributeValueMemberB{Value: cp}, nil
	case *streamtypes.AttributeValueMemberBOOL:
		return &ddbtypes.AttributeValueMemberBOOL{Value: av.Value}, nil
	case *streamtypes.AttributeValueMemberNULL:
		return &ddbtypes.AttributeValueMemberNULL{Value: av.Value}, nil
	case *streamtypes.AttributeValueMemberSS:
		cp := append([]string(nil), av.Value...)
		return &ddbtypes.AttributeValueMemberSS{Value: cp}, nil
	case *streamtypes.AttributeValueMemberNS:
		cp := append([]string(nil), av.Value...)
		return &ddbtypes.AttributeValueMemberNS{Value: cp}, nil
	case *streamtypes.AttributeValueMemberBS:
		cp := make([][]byte, len(av.Value))
		for i, b := range av.Value {
			elem := make([]byte, len(b))
			copy(elem, b)
			cp[i] = elem
		}
		return &ddbtypes.AttributeValueMemberBS{Value: cp}, nil
	case *streamtypes.AttributeValueMemberL:
		list := make([]ddbtypes.AttributeValue, len(av.Value))
		for i, elem := range av.Value {
			converted, err := convertAttributeValue(elem)
			if err != nil {
				return nil, fmt.Errorf("list[%d]: %w", i, err)
			}
			list[i] = converted
		}
		return &ddbtypes.AttributeValueMemberL{Value: list}, nil
	case *streamtypes.AttributeValueMemberM:
		m, err := ConvertAttributeMap(av.Value)
		if err != nil {
			return nil, err
		}
		if m == nil {
			m = map[string]ddbtypes.AttributeValue{}
		}
		return &ddbtypes.AttributeValueMemberM{Value: m}, nil
	default:
		return nil, fmt.Errorf("unsupported attribute value type %T", v)
	}
}

// CanonicalKey produces a deterministic string identifier for a DynamoDB item's key map.
func CanonicalKey(keys map[string]streamtypes.AttributeValue) string {
	if len(keys) == 0 {
		return ""
	}
	names := make([]string, 0, len(keys))
	for k := range keys {
		names = append(names, k)
	}
	sort.Strings(names)

	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteByte('\x00')
		}
		b.WriteString(name)
		b.WriteByte('=')
		switch av := keys[name].(type) {
		case *streamtypes.AttributeValueMemberS:
			b.WriteString("S:")
			b.WriteString(av.Value)
		case *streamtypes.AttributeValueMemberN:
			b.WriteString("N:")
			b.WriteString(av.Value)
		case *streamtypes.AttributeValueMemberB:
			b.WriteString("B:")
			b.WriteString(base64.StdEncoding.EncodeToString(av.Value))
		default:
			fmt.Fprintf(&b, "?%T", keys[name])
		}
	}
	return b.String()
}
