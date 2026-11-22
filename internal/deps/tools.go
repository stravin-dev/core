//go:build tools

// Package deps pins core Stravin dependencies in go.mod so parallel Wave 2/3
// development avoids go.mod merge conflicts without bloating stub binaries.
package deps

import (
	_ "github.com/aws/aws-sdk-go-v2/aws"
	_ "github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	_ "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	_ "github.com/aws/aws-sdk-go-v2/service/kms"
	_ "github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/tetratelabs/wazero"
	_ "go.opentelemetry.io/otel"
	_ "go.opentelemetry.io/otel/propagation"
	_ "go.opentelemetry.io/otel/sdk/trace"
	_ "golang.org/x/crypto/ssh"
	_ "google.golang.org/grpc"
)
