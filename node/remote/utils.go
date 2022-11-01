package remote

import (
	"context"
	"regexp"
	"strconv"

	grpctypes "github.com/cosmos/cosmos-sdk/types/grpc"
	"google.golang.org/grpc/metadata"

	"google.golang.org/grpc"
)

var (
	HTTPProtocols = regexp.MustCompile("https?://")
)

// GetHeightRequestContext adds the height to the context for querying the state at a given height
func GetHeightRequestContext(context context.Context, height int64) context.Context {
	return metadata.AppendToOutgoingContext(
		context,
		grpctypes.GRPCBlockHeightHeader,
		strconv.FormatInt(height, 10),
	)
}

// MustCreateGrpcConnection creates a new gRPC connection using the provided configuration and panics on error
func MustCreateGrpcConnection(cfg *GRPCConfig) *grpc.ClientConn {
	grpConnection, err := CreateGrpcConnection(cfg)
	if err != nil {
		panic(err)
	}
	return grpConnection
}

// CreateGrpcConnection creates a new gRPC client connection from the given configuration
func CreateGrpcConnection(cfg *GRPCConfig) (*grpc.ClientConn, error) {

	address := HTTPProtocols.ReplaceAllString(cfg.Address, "")
	return grpc.Dial(
		address,
		grpc.WithInsecure(),
	)
}
