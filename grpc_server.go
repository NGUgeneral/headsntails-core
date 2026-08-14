package main

import (
	"context"
	"strings"

	sdk "github.com/NGUgeneral/headsntails-sdk/go/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FlagServiceServer struct {
	sdk.UnimplementedFlagServiceServer
	engine *Engine
}

func NewFlagServiceServer(engine *Engine) *FlagServiceServer {
	return &FlagServiceServer{engine: engine}
}

func (s *FlagServiceServer) GetFlag(ctx context.Context, req *sdk.GetFlagRequest) (*sdk.GetFlagResponse, error) {
	flagKey := req.GetFlagKey()
	if flagKey == "" {
		return nil, status.Error(codes.InvalidArgument, "flag_key is required")
	}

	var service, key string
	if parts := strings.SplitN(flagKey, ":", 2); len(parts) == 2 {
		service, key = parts[0], parts[1]
	} else if parts := strings.SplitN(flagKey, "/", 2); len(parts) == 2 {
		service, key = parts[0], parts[1]
	} else {
		key = flagKey
	}

	enabled := s.engine.GetFlag(service, key)

	return &sdk.GetFlagResponse{
		FlagKey: flagKey,
		Enabled: enabled,
	}, nil
}

func (s *FlagServiceServer) GetAllFlags(ctx context.Context, req *sdk.GetAllFlagsRequest) (*sdk.GetAllFlagsResponse, error) {
	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()

	var responses []*sdk.GetFlagResponse
	for compositeKey, enabled := range s.engine.flags {
		responses = append(responses, &sdk.GetFlagResponse{
			FlagKey: compositeKey,
			Enabled: enabled,
		})
	}

	return &sdk.GetAllFlagsResponse{
		Flags: responses,
	}, nil
}
